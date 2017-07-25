package raftnode

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/jj-tyro/raftmcd/raftfsm"
	"github.com/silenceper/pool"
)

var log *logs.BeeLogger

func init() {
	log = logs.GetBeeLogger()
}

type raftStore interface {
	Close() error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(idx uint64, log *raft.Log) error
	StoreLog(log *raft.Log) error
	StoreLogs(logs []*raft.Log) error
	DeleteRange(min, max uint64) error
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
}

type Options struct {
	Addr       string
	Tn         raft.StreamLayer
	Rfdir      string
	Peers      []string
	Snapretain int
}

type Node struct {
	mu        sync.RWMutex
	raft      *raft.Raft
	logstore  raftStore
	peerstore raft.PeerStore
	snapstore raft.SnapshotStore
	fsmstore  raft.FSM
	trans     raft.Transport
}

func NewNode(opts *Options, pool *pool.Pool) (node *Node, err error) {
	n := &Node{}
	defer func() {
		if err != nil {
			n.Close()
		}
	}()

	n.logstore, err = raftboltdb.NewBoltStore(filepath.Join(opts.Rfdir, "raft.db"))
	if err != nil {
		log.Error("Create raft node's store failed: %s", err.Error())
		return
	}

	n.snapstore, err = raft.NewFileSnapshotStore(opts.Rfdir, opts.Snapretain, log)
	if err != nil {
		n.Close()
		log.Error("Create raft node's snapshot store failed %s", err.Error())
		return
	}

	n.trans = raft.NewNetworkTransport(opts.Tn, 3, 2*time.Second, log)
	if n.trans == nil {
		log.Error("Create raft node's Tcp transport failed")
		return
	}

	n.peerstore = raft.NewJSONPeers(opts.Rfdir, n.trans)
	n.peerstore.SetPeers(opts.Peers)
	log.Debug("opts.Peers %v len %v", opts.Peers, len(opts.Peers))
	ps, _ := n.peerstore.Peers()
	log.Debug("Peers %v len %v", ps, len(ps))

	rfConf := raft.DefaultConfig()
	rfConf.LogOutput = log

	if rfConf.EnableSingleNode && len(ps) <= 1 {
		rfConf.EnableSingleNode = true
		rfConf.DisableBootstrapAfterElect = false
	}

	n.fsmstore, err = raftfsm.NewMcdStore(pool)
	if err != nil {
		log.Error("Create raft node's memcached store failed: %s", err.Error())
		return
	}

	n.raft, err = raft.NewRaft(rfConf, n.fsmstore, n.logstore, n.logstore, n.snapstore, n.peerstore, n.trans)
	if err != nil {
		log.Error("Create raft node failed: %s", err.Error())
		return
	}

	node = n

	return
}

func (n *Node) Apply(data []byte, tmout time.Duration) (val interface{}, err error) {
	f := n.raft.Apply(data, tmout)

	if err = f.Error(); err != nil {
		return
	}

	val = f.Response()

	return
}

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// shutdown the raft, but do not handle the future error. :PPA:
	if n.raft != nil {
		n.raft.Shutdown()
	}

	if n.logstore != nil {
		n.logstore.Close()
	}

	if n.trans != nil {

	}

	return nil
}
