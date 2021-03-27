package core

import "sync"

// ========== raft 保存的数据 ==========
type RaftState struct {
	Term     int
	VotedFor NodeId
	Entries  []Entry
}

func (rs RaftState) toHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:      rs.Term,
		votedFor:  rs.VotedFor,
		entries:   rs.Entries,
		persister: persister,
	}
}

// ========== 状态持久化器接口，由用户实现 ==========
type RaftStatePersister interface {
	// 每次 raft 的状态改变，都会调用此方法
	// entries 字段在变化之后进行持久化即可
	SaveRaftState(RaftState) error
	// 没有时返回空对象
	LoadRaftState() (RaftState, error)
}

// ========== 保存的快照数据 ==========
type Snapshot struct {
	LastIndex int
	LastTerm  int
	Data      []byte
}

// ========== 快照持久化器接口，由用户实现 ==========
type SnapshotPersister interface {
	// 保存快照时调用
	SaveSnapshot(Snapshot) error
	// 若没有需返回空对象
	LoadSnapshot() (Snapshot, error)
}

// RaftStatePersister 接口的内存实现，开发测试用
type inMemRaftStatePersister struct {
	raftState RaftState
	mu        sync.Mutex
}

func newImMemRaftStatePersister() *inMemRaftStatePersister {
	return &inMemRaftStatePersister{
		raftState: RaftState{
			Term:     0,
			VotedFor: "",
			Entries:  make([]Entry, 0),
		},
	}
}

func (ps *inMemRaftStatePersister) SaveRaftState(state RaftState) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftState = state
	return nil
}

func (ps *inMemRaftStatePersister) LoadRaftState() (RaftState, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftState, nil
}

// SnapshotPersister 接口的内存实现，开发测试用
type inMemSnapshotPersister struct {
	snapshot Snapshot
	mu       sync.Mutex
}

func newInMemSnapshotPersister() *inMemSnapshotPersister {
	return &inMemSnapshotPersister{}
}

func (ps *inMemSnapshotPersister) SaveSnapshot(snapshot Snapshot) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
	return nil
}

func (ps *inMemSnapshotPersister) LoadSnapshot() (Snapshot, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot, nil
}
