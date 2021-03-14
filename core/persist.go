package core

// ========== 状态持久化器接口，由用户实现 ==========
type RaftStatePersister interface {
	// 每次 raft 的状态改变，都会调用此方法
	// entries 字段在变化之后进行持久化即可
	SaveRaftState(RaftState) error
	// 没有时返回空对象
	LoadRaftState() (RaftState, error)
}

// ========== 快照持久化器接口，由用户实现 ==========
type SnapshotPersister interface {
	// 保存快照时调用
	SaveSnapshot(Snapshot) error
	// 若没有需返回空对象
	LoadSnapshot() (Snapshot, error)
}

// ========== raft 保存的数据 ==========
type RaftState struct {
	Term     int
	VotedFor NodeId
	Entries  []Entry
}

func newRaftState() RaftState {
	return RaftState{
		Term:     1,
		VotedFor: "",
		Entries:  make([]Entry, 0),
	}
}

func (rs RaftState) toHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:      rs.Term,
		votedFor:  rs.VotedFor,
		entries:   rs.Entries,
		persister: persister,
	}
}

// ========== 保存的快照数据 ==========
type Snapshot struct {
	LastIndex int
	LastTerm  int
	Data      []byte
}
