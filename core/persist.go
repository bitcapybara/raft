package core

import "encoding/gob"

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

type persister struct {
	raftStatePath string
	snapshotPath string
}

func NewPersister(fsm Fsm) *persister {
	gob.Register(fsm)
	return &persister{}
}

// 持久化 raftState
func (ps *persister) saveRaftState(state raftState) error {
	return nil
}

// 加载 raftState
func (ps *persister) loadRaftState() (raftState, error) {
	return raftState{}, nil
}

// 保存快照
func (ps *persister) saveSnapshot(ss snapshot) error {
	return nil
}

// 加载快照
func (ps *persister) loadSnapshot() (snapshot, error) {
	return snapshot{}, nil
}


