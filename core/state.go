package core

import "sync"

// ==================== RoleState ====================

const (
	Leader    RoleStage = iota // 领导者
	Candidate                  // 候选者
	Follower                   // 追随者
)

// 角色类型
type RoleStage uint8

type RoleState struct {
	roleStage RoleStage // 节点当前角色
	mu        sync.RWMutex
}

func NewRoleState() *RoleState {
	return &RoleState{
		roleStage: Follower,
	}
}

func (st *RoleState) setRoleStage(stage RoleStage) {
	st.mu.Lock()
	st.roleStage = stage
	st.mu.Unlock()
}

func (st *RoleState) getRoleStage() RoleStage {
	st.mu.RLock()
	stage := st.roleStage
	st.mu.RUnlock()
	return stage
}

// ==================== HardState ====================

// 日志条目
type Entry struct {
	Index int    // 此条目的索引
	Term  int    // 日志项所在term
	Data  []byte // 状态机命令
}

type NodeId string

type NodeAddr string

// 需要持久化存储的状态
type HardState struct {
	term      int                // 当前时刻所处的 term
	votedFor  NodeId             // 当前任期获得选票的 Candidate
	entries   []Entry            // 当前节点保存的日志
	persister RaftStatePersister // 持久化器
	mu        sync.Mutex
}

func NewHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:      1,
		votedFor:  "",
		entries:   []Entry{},
		persister: persister,
	}
}

func (st *HardState) lastLogIndex() int {
	st.mu.Lock()
	lastIndex := len(st.entries) - 1
	st.mu.Unlock()
	return lastIndex
}

func (st *HardState) currentTerm() int {
	st.mu.Lock()
	term := st.term
	st.mu.Unlock()
	return term
}

func (st *HardState) logEntryTerm(index int) int {
	st.mu.Lock()
	term := st.entries[index].Term
	st.mu.Unlock()
	return term
}

func (st *HardState) setTerm(term int) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.term = term
	st.votedFor = ""
	return st.persist()
}

func (st *HardState) vote(id NodeId) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.votedFor = id
	return st.persist()
}

func (st *HardState) persist() error {
	raftState := RaftState{
		Term:     st.term,
		VotedFor: st.votedFor,
		Entries:  st.entries,
	}
	return st.persister.SaveRaftState(raftState)
}

// ==================== SoftState ====================

// 保存在内存中的实时状态
type SoftState struct {
	commitIndex int // 已经提交的最大的日志索引，由当前节点维护
	lastApplied int // 应用到状态机的最后一个日志索引
	mu          sync.Mutex
}

func NewSoftState() *SoftState {
	return &SoftState{
		commitIndex: 0,
		lastApplied: 0,
	}
}

func (st *SoftState) softCommitIndex() int {
	st.mu.Lock()
	commitIndex := st.commitIndex
	st.mu.Unlock()
	return commitIndex
}

// ==================== PeerState ====================

// 对等节点状态和路由表
type PeerState struct {
	peers  map[NodeId]NodeAddr // 所有节点
	me     NodeId              // 当前节点在 peers 中的索引
	leader NodeId              // 当前 leader 在 peers 中的索引
	mu     sync.Mutex
}

func NewPeerState(peers map[NodeId]NodeAddr, me NodeId) *PeerState {
	return &PeerState{
		peers:  peers,
		me:     me,
		leader: "",
	}
}

func (st *PeerState) leaderIsMe() bool {
	st.mu.Lock()
	isLeader := st.leader == st.me
	st.mu.Unlock()
	return isLeader
}

func (st *PeerState) majority() int {
	st.mu.Lock()
	num := len(st.peers)/2 + 1
	st.mu.Unlock()
	return num
}
func (st *PeerState) peersMap() map[NodeId]NodeAddr {
	st.mu.Lock()
	peers := st.peers
	st.mu.Unlock()
	return peers
}

func (st *PeerState) isMe(id NodeId) bool {
	st.mu.Lock()
	isMe := id == st.me
	st.mu.Unlock()
	return isMe
}

func (st *PeerState) identity() NodeId {
	st.mu.Lock()
	me := st.me
	st.mu.Unlock()
	return me
}

func (st *PeerState) setLeader(id NodeId) {
	st.mu.Lock()
	st.leader = id
	st.mu.Unlock()
}

// ==================== LeaderState ====================

// 节点是 Leader 时，保存在内存中的状态
type LeaderState struct {

	// 下一次要发送给各节点的日志索引。由 Leader 维护，初始值为 Leader 最后一个日志的索引 + 1
	nextIndex map[NodeId]int

	// 已经复制到各节点的最大的日志索引。由 Leader 维护，初始值为0
	matchIndex map[NodeId]int

	mu sync.Mutex
}

func NewLeaderState() *LeaderState {
	return &LeaderState{
		nextIndex:  make(map[NodeId]int),
		matchIndex: make(map[NodeId]int),
	}
}

func (st *LeaderState) peerMatchIndex(id NodeId) int {
	st.mu.Lock()
	index := st.matchIndex[id]
	st.mu.Unlock()
	return index
}

func (st *LeaderState) setMatchAndNextIndex(id NodeId, matchIndex, nextIndex int) {
	st.mu.Lock()
	st.matchIndex[id] = matchIndex
	st.nextIndex[id] = nextIndex
	st.mu.Unlock()
}
