package core

import (
	"encoding/gob"
)

type Persister interface {
	// 持久化 raftState
	SaveRaftState(raftState) error

	// 加载 raftState
	LoadRaftState() (raftState, error)

	// 持久化快照
	SaveSnapshot(ss snapshot) error

	// 加载快照
	LoadSnapshot() (snapshot, error)
}

type raftState struct {
	Term     int
	VotedFor int
	Entries  []entry
}

type snapshot struct {
	lastIndex int
	lastTerm  int
	state     Fsm
}

// 持久化器的默认实现，保存在文件中
type defaultPersister struct {
	filePath string
}

func newPersister(fsm Fsm) *defaultPersister {
	gob.Register(fsm)
	dp := new(defaultPersister)
	dp.filePath = "./persist.store"
	return dp
}

// 持久化 raftState
func (ps *defaultPersister) SaveRaftState(state raftState) error {
	return nil
}

// 加载 raftState
func (ps *defaultPersister) LoadRaftState() (raftState, error) {
	return raftState{}, nil
}

// 持久化快照
func (ps *defaultPersister) SaveSnapshot(ss snapshot) error {
	return nil
}

// 加载快照
func (ps *defaultPersister) LoadSnapshot() (snapshot, error) {
	return snapshot{}, nil
}


