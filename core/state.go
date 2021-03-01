package core

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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
}

func newRoleState() *RoleState {
	return &RoleState{
		roleStage: Follower,
	}
}

func (st *RoleState) setRoleStage(stage RoleStage) {
	st.roleStage = stage
}

func (st *RoleState) getRoleStage() RoleStage {
	return st.roleStage
}

// ==================== HardState ====================

// 日志条目
type Entry struct {
	Index int    // 此条目的逻辑索引， 从 1 开始
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
}

func NewHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:      1,
		votedFor:  "",
		entries:   make([]Entry, 0),
		persister: persister,
	}
}

func (st *HardState) lastEntryIndex() int {
	lastLogIndex := len(st.entries) - 1
	if lastLogIndex < 0 {
		return 0
	} else {
		return st.entries[lastLogIndex].Index
	}
}

func (st *HardState) currentTerm() int {
	return st.term
}

// todo 传入的必须是物理索引
func (st *HardState) logEntryTerm(index int) int {
	if len(st.entries) - 1 < index {
		return 0
	}
	return st.entries[index].Term
}

func (st *HardState) logLength() int {
	return len(st.entries)
}

func (st *HardState) setTerm(term int) error {
	if st.term >= term {
		return nil
	}
	st.term = term
	st.votedFor = ""
	return st.persist()
}

func (st *HardState) vote(id NodeId) error {
	if st.votedFor == id {
		return nil
	}
	st.votedFor = id
	return st.persist()
}

func (st *HardState) persist() error {
	raftState := RaftState{
		Term:     st.term,
		VotedFor: st.votedFor,
		Entries:  st.entries,
	}
	err := st.persister.SaveRaftState(raftState)
	if err != nil {
		return fmt.Errorf("raft 状态持久化失败：%w", err)
	}
	return nil
}

func (st *HardState) appendEntry(entry Entry) error {
	st.entries = append(st.entries, entry)
	return st.persist()
}

func (st *HardState) logEntry(index int) Entry {
	return st.entries[index]
}

func (st *HardState) voted() NodeId {
	return st.votedFor
}

func (st *HardState) truncateEntries(index int) {
	st.entries = st.entries[:index]
}

func (st *HardState) clearEntries() {
	st.entries = make([]Entry, 0)
}

func (st *HardState) logEntries(start, end int) []Entry {
	return st.entries[start:end]
}

// ==================== SoftState ====================

// 保存在内存中的实时状态
type SoftState struct {
	commitIndex int // 已经提交的最大的日志索引，由当前节点维护，初始化为0
	lastApplied int // 应用到状态机的最后一个日志索引
	mu          sync.Mutex
}

func newSoftState() *SoftState {
	return &SoftState{
		commitIndex: 0,
		lastApplied: 0,
	}
}

func (st *SoftState) softCommitIndex() int {
	return st.commitIndex
}

func (st *SoftState) setCommitIndex(index int) {
	st.commitIndex = index
}

func (st *SoftState) setLastApplied(index int) {
	st.lastApplied = index
}

func (st *SoftState) lastAppliedAdd() int {
	st.lastApplied += 1
	return st.lastApplied
}

func (st *SoftState) softLastApplied() int {
	return st.lastApplied
}

// ==================== PeerState ====================

// 对等节点状态和路由表
type PeerState struct {
	peers  map[NodeId]NodeAddr // 所有节点
	me     NodeId              // 当前节点在 peers 中的索引
	leader NodeId              // 当前 leader 在 peers 中的索引
}

func newPeerState(peers map[NodeId]NodeAddr, me NodeId) *PeerState {
	return &PeerState{
		peers:  peers,
		me:     me,
		leader: "",
	}
}

func (st *PeerState) leaderIsMe() bool {
	return st.leader == st.me
}

func (st *PeerState) majority() int {
	return len(st.peers)/2 + 1
}
func (st *PeerState) peersMap() map[NodeId]NodeAddr {
	return st.peers
}

func (st *PeerState) peersCnt() int {
	return len(st.peers)
}

func (st *PeerState) isMe(id NodeId) bool {
	return id == st.me
}

func (st *PeerState) myId() NodeId {
	return st.me
}

func (st *PeerState) setLeader(id NodeId) {
	st.leader = id
}

func (st *PeerState) leaderId() NodeId {
	return st.leader
}

func (st *PeerState) getLeader() server {
	return server{
		id: st.leader,
		addr: st.peers[st.leader],
	}
}

// ==================== LeaderState ====================

// 节点是 Leader 时，保存在内存中的状态
type LeaderState struct {

	// 下一次要发送给各节点的日志索引。由 Leader 维护，初始值为 Leader 最后一个日志的索引 + 1
	nextIndex map[NodeId]int

	// 已经复制到各节点的最大的日志索引。由 Leader 维护，初始值为0
	matchIndex map[NodeId]int
}

func newLeaderState() *LeaderState {
	return &LeaderState{
		nextIndex:  make(map[NodeId]int),
		matchIndex: make(map[NodeId]int),
	}
}

func (st *LeaderState) peerMatchIndex(id NodeId) int {
	return st.matchIndex[id]
}

func (st *LeaderState) setMatchAndNextIndex(id NodeId, matchIndex, nextIndex int) {
	st.matchIndex[id] = matchIndex
	st.nextIndex[id] = nextIndex
}

func (st *LeaderState) peerNextIndex(id NodeId) int {
	return st.nextIndex[id]
}

func (st *LeaderState) setNextIndex(id NodeId, index int) {
	st.nextIndex[id] = index
}

// ==================== timerState ====================

type timerType uint8

const (
	Election timerType = iota
	Heartbeat
)

type timerState struct {
	timerType timerType       // 计时器类型
	timer     *time.Timer     // 超时计时器

	electionMinTimeout int // 最小选举超时时间
	electionMaxTimeout int // 最大选举超时时间
	heartbeatTimeout   int // 心跳间隔时间
}

func newTimerState(config Config) *timerState {
	return &timerState{
		electionMinTimeout: config.ElectionMinTimeout,
		electionMaxTimeout: config.ElectionMaxTimeout,
		heartbeatTimeout:   config.HeartbeatTimeout,
	}
}

func (st *timerState) initTimerState() {
	st.timer = time.NewTimer(st.electionDuration())
	st.timerType = Election
}

func (st *timerState) getTimerType() timerType {
	return st.timerType
}

func (st *timerState) setElectionTimer() {
	st.timer.Reset(st.electionDuration())
}

func (st *timerState) resetElectionTimer() {
	st.timer.Stop()
	st.setElectionTimer()
}

func (st *timerState) resetHeartbeatTimer() {
	st.timer.Stop()
	st.setHeartbeatTimer()
}

func (st *timerState) setHeartbeatTimer() {
	st.timer.Reset(st.heartbeatDuration())
}

func (st *timerState) electionDuration() time.Duration {
	randTimeout := rand.Intn(st.electionMaxTimeout-st.electionMinTimeout) + st.electionMinTimeout
	return time.Millisecond * time.Duration(randTimeout)
}

func (st *timerState) heartbeatDuration() time.Duration {
	return time.Millisecond * time.Duration(st.heartbeatTimeout)
}

// ==================== snapshotState ====================

type snapshotState struct {
	snapshot     *Snapshot
	persister    SnapshotPersister
	maxLogLength int
}

func newSnapshotState(config Config) *snapshotState {
	persister := config.SnapshotPersister
	snapshot, err := persister.LoadSnapshot()
	if err != nil {
		log.Fatalln(fmt.Errorf("加载快照失败：%w", err))
	}
	return &snapshotState{
		snapshot:     &snapshot,
		persister:    persister,
		maxLogLength: config.MaxLogLength,
	}
}

func (st *snapshotState) save(snapshot Snapshot) error {
	err := st.persister.SaveSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("保存快照失败：%w", err)
	}
	st.snapshot = &snapshot
	return nil
}

func (st *snapshotState) needGenSnapshot(commitIndex int) bool {
	need := commitIndex - st.snapshot.LastIndex >= st.maxLogLength
	return need
}

func (st *snapshotState) lastIndex() int {
	return st.snapshot.LastIndex
}

func (st *snapshotState) lastTerm() int {
	return st.snapshot.LastTerm
}

func (st *snapshotState) getSnapshot() *Snapshot {
	return st.snapshot
}
