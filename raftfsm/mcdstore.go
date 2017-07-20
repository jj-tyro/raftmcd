package raftfsm

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/astaxie/beego/logs"
	"github.com/dustin/gomemcached"
	mcdcli "github.com/dustin/gomemcached/client"

	"github.com/hashicorp/raft"
	"github.com/silenceper/pool"
)

var log *logs.BeeLogger

func init() {
	log = logs.GetBeeLogger()
}

type McdStore struct {
	cliPool *pool.Pool
}

func NewMcdStore(p *pool.Pool) (s *McdStore, err error) {
	return &McdStore{
		cliPool: p,
	}, nil
}

func (m *McdStore) Apply(l *raft.Log) interface{} {

	if len(l.Data) == 0 {
		// blank data
		log.Error("Raft log data is empty!")
		return nil
	}

	var req gomemcached.MCRequest

	dec := gob.NewDecoder(bytes.NewBuffer(l.Data))
	if err := dec.Decode(&req); err != nil {
		log.Error("Decode memcached request failed: %s", err.Error())
		return nil
	}

	log.Debug("Send request to memcached server: %v", req)

	rv, err2 := m.transfer(&req)
	if err2 != nil {
		log.Error("Transfer request to memcached server failed: %s", err2.Error())
		return nil
	}

	log.Debug("Recv response from memcached server: %v", rv)
	return rv

	/*buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)

	if err := enc.Encode(*rv); err != nil {
		log.Printf("Encode response failed: %v", err)
		return nil
	}

	return buf.Bytes()
	*/

}

func (m *McdStore) Snapshot() (fsnap raft.FSMSnapshot, err error) {
	//TODO
	return nil, nil
}

func (m *McdStore) Restore(io.ReadCloser) (err error) {
	//TODO
	return nil
}

func (m *McdStore) transfer(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error) {
	var c interface{}

	if c, err = (*m.cliPool).Get(); err != nil {
		return
	}

	cli := c.(*mcdcli.Client)
	rv, err = cli.Send(req)

	(*m.cliPool).Close(c)

	return
}
