package core

import "log"

type raft struct {
	roleState   *RoleState   // 当前节点的角色
	persister   *Persister   // 持久化器
	fsm         *Fsm         // 客户端状态机
	hardState   *HardState   // 需要持久化存储的状态
	softState   *SoftState   // 保存在内存中的实时状态
	peerState   *PeerState   // 对等节点状态和路由表
	leaderState *LeaderState // 节点是 Leader 时，保存在内存中的状态
}

func NewRaft(config Config) *raft {
	pst := config.Persister
	hardState, err := (*pst).LoadHardState()
	if err != nil {
		log.Println(err)
		hardState = NewHardState()
	}

	return &raft{
		roleState: NewRoleState(),
		persister: pst,
		fsm:       config.Fsm,
		hardState: hardState,
		softState: NewSoftState(),
		peerState: NewPeerState(config.peers, config.me),
		leaderState: NewLeaderState(),
	}
}

func (rf *raft) appendEntry(req AppendEntry, ch chan AppendEntryReply) {

}

func (rf *raft) requestVote(req RequestVote, ch chan RequestVoteReply) {

}

func (rf *raft) installSnapshot(req InstallSnapshot, ch chan InstallSnapshotReply) {

}

func (rf *raft) isLeader() bool {
	return rf.roleState.getRoleStage() == Leader
}

func (rf *raft) heartbeat() {

}

func (rf *raft) newElection() {

}
