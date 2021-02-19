package core

import (
	"log"
	"sync"
)

type raft struct {
	roleState   *RoleState        // 当前节点的角色
	persister   SnapshotPersister // 快照生成器
	fsm         Fsm               // 客户端状态机
	hardState   *HardState        // 需要持久化存储的状态
	softState   *SoftState        // 保存在内存中的实时状态
	peerState   *PeerState        // 对等节点状态和路由表
	leaderState *LeaderState      // 节点是 Leader 时，保存在内存中的状态
	mu          sync.Mutex
}

func NewRaft(config Config) *raft {
	raftPst := config.RaftStatePersister
	snptPst := config.SnapshotPersister
	raftState, err := raftPst.LoadHardState()
	var hardState HardState
	if err != nil {
		log.Println(err)
		hardState = NewHardState(raftPst)
	} else {
		hardState = raftState.toHardState(raftPst)
	}

	return &raft{
		roleState:   NewRoleState(),
		persister:   snptPst,
		fsm:         config.Fsm,
		hardState:   &hardState,
		softState:   NewSoftState(),
		peerState:   NewPeerState(config.Peers, config.Me),
		leaderState: NewLeaderState(),
	}
}

func (rf *raft) isLeader() bool {
	roleStage := rf.roleState.getRoleStage()
	leaderIsMe := rf.peerState.leaderIsMe()
	return roleStage == Leader && leaderIsMe
}

func (rf *raft) commitIndex() int {
	return rf.softState.softCommitIndex()
}

func (rf *raft) lastLogIndex() int {
	return rf.hardState.lastLogIndex()
}

func (rf *raft) matchIndex(id NodeId) int {
	return rf.leaderState.peerMatchIndex(id)
}

func (rf *raft) term() int {
	return rf.hardState.currentTerm()
}

func (rf *raft) majority() int {
	return rf.peerState.majority()
}

// 降级为 Follower
func (rf *raft) degrade(term int) error {
	if rf.roleState.getRoleStage() == Follower {
		return nil
	}
	// 更新状态
	rf.mu.Lock()
	rf.mu.Unlock()
	rf.roleState.setRoleStage(Follower)
	return rf.hardState.setTerm(term)
}

// 更新自身 term
func (rf *raft) setTerm(term int) error {
	if rf.term() == term {
		return nil
	}
	// 更新状态
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.hardState.setTerm(term)
}
