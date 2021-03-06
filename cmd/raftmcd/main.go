package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"path/filepath"

	"github.com/astaxie/beego/logs"
	"github.com/dustin/gomemcached"
	mcdcli "github.com/dustin/gomemcached/client"
	mcdsvr "github.com/dustin/gomemcached/server"
	"github.com/jj-tyro/raftmcd/raftnode"
	"github.com/rqlite/rqlite/tcp"
	"github.com/silenceper/pool"
)

type mcdCmdType int

const (
	readCmd = iota + 1
	writeCmd
	miscCmd
	unkwoned
)

var log *logs.BeeLogger
var mcdCmdTypeMap map[gomemcached.CommandCode]mcdCmdType

func init() {
	log = logs.GetBeeLogger()
	mcdCmdTypeMap = map[gomemcached.CommandCode]mcdCmdType{
		gomemcached.GET:        readCmd,
		gomemcached.SET:        writeCmd,
		gomemcached.ADD:        writeCmd,
		gomemcached.REPLACE:    writeCmd,
		gomemcached.DELETE:     writeCmd,
		gomemcached.INCREMENT:  writeCmd,
		gomemcached.DECREMENT:  writeCmd,
		gomemcached.FLUSH:      writeCmd,
		gomemcached.GETQ:       readCmd,
		gomemcached.NOOP:       miscCmd,
		gomemcached.VERSION:    miscCmd,
		gomemcached.GETK:       readCmd,
		gomemcached.GETKQ:      readCmd,
		gomemcached.APPEND:     writeCmd,
		gomemcached.PREPEND:    writeCmd,
		gomemcached.STAT:       miscCmd,
		gomemcached.SETQ:       writeCmd,
		gomemcached.ADDQ:       writeCmd,
		gomemcached.REPLACEQ:   writeCmd,
		gomemcached.DELETEQ:    writeCmd,
		gomemcached.INCREMENTQ: writeCmd,
		gomemcached.DECREMENTQ: writeCmd,
		gomemcached.FLUSHQ:     writeCmd,
		gomemcached.APPENDQ:    writeCmd,
		gomemcached.PREPENDQ:   writeCmd,
		gomemcached.RGET:       readCmd,
		gomemcached.RSET:       writeCmd,
		gomemcached.RSETQ:      writeCmd,
		gomemcached.RAPPEND:    writeCmd,
		gomemcached.RAPPENDQ:   writeCmd,
		gomemcached.RPREPEND:   writeCmd,
		gomemcached.RPREPENDQ:  writeCmd,
		gomemcached.RDELETE:    writeCmd,
		gomemcached.RDELETEQ:   writeCmd,
		gomemcached.RINCR:      writeCmd,
		gomemcached.RINCRQ:     writeCmd,
		gomemcached.RDECR:      writeCmd,
		gomemcached.RDECRQ:     writeCmd,
	}
}

type ReqHandler struct {
	mu      sync.RWMutex
	node    *raftnode.Node
	cliPool *pool.Pool
}

func (h *ReqHandler) HandleMessage(w io.Writer, req *gomemcached.MCRequest) (resp *gomemcached.MCResponse) {
	var err error

	log.Debug("Request from client: %v", req)

	cmdType, ok := mcdCmdTypeMap[req.Opcode]
	if !ok {
		cmdType = unkwoned
	}

	if req.Opcode.IsQuiet() {
		resp = &gomemcached.MCResponse{
			Opcode: req.Opcode,
			Status: gomemcached.UNKNOWN_COMMAND,
			Opaque: req.Opaque,
			Cas:    req.Cas,
			Key:    req.Key,
			Body:   []byte("UnSupported Command"),
		}
		return
	}

	if cmdType != writeCmd {
		resp, err = h.transfer(req)
	} else {
		buf := bytes.NewBuffer(nil)
		enc := gob.NewEncoder(buf)

		if err = enc.Encode(*req); err != nil {
			log.Error("Encode Request to raft log data failed: %s", err.Error())
			return nil
		}

		var rv interface{}
		rv, err = h.node.Apply(buf.Bytes(), time.Second)
		if err != nil {
			log.Error("Encode Request to raft log data failed: %s", err.Error())
		}

		if rv == nil {
			log.Error("Apply raft log data return null")
			err = errors.New("Invalid Response Value")
		}

		resp = rv.(*gomemcached.MCResponse)
	}

	log.Debug("Response to client: %v", resp)

	return
}

func (h *ReqHandler) transfer(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error) {
	var c interface{}

	if c, err = (*h.cliPool).Get(); err != nil {
		return
	}

	cli := c.(*mcdcli.Client)
	rv, err = cli.Send(req)

	(*h.cliPool).Close(c)

	return
}

// Close closes the node
func (h *ReqHandler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// shutdown the raft, but do not handle the future error. :PPA:

	return nil
}

func connectionHandler(s net.Conn, handler mcdsvr.RequestHandler) {
	defer s.Close()

	_ = mcdsvr.HandleIO(s, handler)
}

func waitForConnections(ls net.Listener, handler mcdsvr.RequestHandler) {

	for {
		s, e := ls.Accept()
		if e == nil {
			log.Debug("Got a connection from %v", s.RemoteAddr())
			go connectionHandler(s, handler)
		} else {
			log.Debug("Error accepting from %s", ls)
		}
	}
}

const (
	muxRaftHeader = 1 // Raft consensus communications
	muxMetaHeader = 2 // Cluster meta communications
)

const name = `raftmcd`
const desc = `A proxy for memcached using raft consensus to replicate data within a cluster.`

func main() {
	var port *int = flag.Int("port", 11200, "Port on which to listen")
	var mcd_addr *string = flag.String("mcd_addr", "localhost", "Ip address on which to backend memcached server listen")
	var mcd_port *int = flag.Int("mcd_port", 11211, "Port on which to backend memcached server listen")
	var raft_addr *string = flag.String("raft_addr", "localhost", "Ip address on which to raft transport listen")
	var raft_port *int = flag.Int("raft_port", 4021, "Port on which to raft memcached server listen")
	var peers *string = flag.String("peers", "", "raft peers that want to join")
	var dir *string = flag.String("dir", "/tmp/rmcd-1", "dir where raft store live in")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n\n", desc)
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments]\n", name)
		flag.PrintDefaults()
	}

	flag.Parse()

	logdir := filepath.Join(*dir, "log")
	if err := os.MkdirAll(logdir, 0755); err != nil {
		log.Critical("Make Directory Failed:  %s", logdir, err.Error())
		panic("Start Failed")
	}
	logfname := filepath.Join(logdir, name+".log")
	logfconf := `{"filename":"` + logfname + `","maxsize":10000000,"daily":true,"maxdays":7,"rotate":true,"level":7}`

	//log.Async(10000)
	log.EnableFuncCallDepth(true)
	log.SetLogFuncCallDepth(2)
	log.SetLogger(logs.AdapterConsole)
	log.SetLogger(logs.AdapterFile, logfconf)

	ls, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Critical("Listen to %d failed:  %s", *port, err.Error())
		panic("Start Failed")
	}

	var opts raftnode.Options
	opts.Addr = fmt.Sprintf("%s:%d", *raft_addr, *raft_port)
	if len(*peers) > 0 {
		opts.Peers = strings.Split(*peers, ",")
	}
	opts.Rfdir = *dir
	opts.Snapretain = 3

	lr, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		log.Critical("Listen to %d failed:  %s", *port, err.Error())
		panic("Start Failed")
	}

	mux, err := tcp.NewMux(lr, nil)
	if err != nil {
		log.Critical("Create MUX on raft listen port %v failed: %s", lr, err.Error())
		panic("Start Failed")
	}
	mux.InsecureSkipVerify = false
	opts.Tn = mux.Listen(muxRaftHeader)
	go mux.Serve()

	factory := func() (interface{}, error) { return mcdcli.Connect("tcp", *mcd_addr+":"+strconv.Itoa(*mcd_port)) }
	close := func(v interface{}) error { return v.(*mcdcli.Client).Close() }

	poolConfig := &pool.PoolConfig{
		InitialCap:  5,
		MaxCap:      30,
		Factory:     factory,
		Close:       close,
		IdleTimeout: 15 * time.Second,
	}

	p, err := pool.NewChannelPool(poolConfig)
	if err != nil {
		log.Critical("Create connection pool to memcached server failed:  %s", err.Error())
		panic("Start Failed")

	}

	n, err := raftnode.NewNode(&opts, &p)
	if err != nil {
		log.Critical("Create raft node failed:  %s", err.Error())
		panic("Start Failed")
	}

	log.Info("Listening on port %d", *port)

	go waitForConnections(ls,
		&ReqHandler{
			node:    n,
			cliPool: &p,
		})

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)

	<-terminate
	n.Close()

	log.Debug("%s server stopped", name)
}
