package core

import (
	"encoding/gob"
)

type Persister interface {
	// 持久化 raftState
	saveRaftState(raftState) error

	// 加载 raftState
	loadRaftState() (raftState, error)

	// 持久化快照
	saveSnapshot(ss snapshot) error

	// 加载快照
	loadSnapshot() (snapshot, error)
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
func (ps *defaultPersister) saveRaftState(state raftState) error {
	return nil
}

// 加载 raftState
func (ps *defaultPersister) loadRaftState() (raftState, error) {
	return raftState{}, nil
}

// 持久化快照
func (ps *defaultPersister) saveSnapshot(ss snapshot) error {
	return nil
}

// 加载快照
func (ps *defaultPersister) loadSnapshot() (snapshot, error) {
	return snapshot{}, nil
}


