package core

import "encoding/gob"

type RaftStatePersister interface {
	// 每次 raft 的状态改变，都会调用此方法
	// entries 字段在变化之后进行持久化即可
	SaveRaftState(RaftState) error
	LoadRaftState() (RaftState, error)
}

type SnapshotPersister interface {
	SaveSnapshot(Snapshot) error
	LoadSnapshot() (Snapshot, error)
}

type RaftState struct {
	Term     int
	VotedFor NodeId
	Entries  []Entry
}

func newRaftState() RaftState {
	return RaftState{
		Term: 1,
		VotedFor: "",
		Entries: make([]Entry, 0),
	}
}

func (rs RaftState) toHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:     rs.Term,
		votedFor: rs.VotedFor,
		entries:  rs.Entries,
		persister: persister,
	}
}

type Snapshot struct {
	LastIndex int
	LastTerm  int
	Data      []byte
}

// todo 持久化器的默认实现，保存在文件中
type DefaultPersister struct {
	FilePath string
}

func NewDefaultPersister(fsm Fsm) *DefaultPersister {
	gob.Register(fsm)
	dp := new(DefaultPersister)
	dp.FilePath = "./persist.store"
	return dp
}

func (d *DefaultPersister) SaveRaftState(state RaftState) error {
	panic("implement me")
}

func (d *DefaultPersister) LoadRaftState() (RaftState, error) {
	panic("implement me")
}

func (d *DefaultPersister) SaveSnapshot(snapshot Snapshot) error {
	panic("implement me")
}

func (d *DefaultPersister) LoadSnapshot() (Snapshot, error) {
	panic("implement me")
}
