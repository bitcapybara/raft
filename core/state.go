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
	mu        sync.RWMutex
}

func newRoleState() *RoleState {
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
		entries:   make([]Entry, 0),
		persister: persister,
	}
}

func (st *HardState) lastLogIndex() int {
	lastIndex := st.logLength() - 1
	if lastIndex <= 0 {
		return 0
	} else {
		return lastIndex
	}
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

func (st *HardState) logLength() int {
	st.mu.Lock()
	length := len(st.entries)
	st.mu.Unlock()
	return length
}

func (st *HardState) setTerm(term int) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.term == term {
		return nil
	}
	st.term = term
	st.votedFor = ""
	return st.persist()
}

func (st *HardState) vote(id NodeId) error {
	st.mu.Lock()
	defer st.mu.Unlock()

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
	st.mu.Lock()
	defer st.mu.Unlock()
	st.entries = append(st.entries, entry)
	return st.persist()
}

func (st *HardState) logEntry(index int) Entry {
	st.mu.Lock()
	entry := st.entries[index]
	st.mu.Unlock()
	return entry
}

func (st *HardState) getVotedFor() NodeId {
	st.mu.Lock()
	votedFor := st.votedFor
	st.mu.Unlock()
	return votedFor
}

func (st *HardState) truncateEntries(index int) {
	st.mu.Lock()
	st.entries = st.entries[:index]
	st.mu.Unlock()
}

func (st *HardState) clearEntries() {
	st.mu.Lock()
	st.entries = make([]Entry, 0)
	st.mu.Unlock()
}

func (st *HardState) logEntries(start, end int) []Entry {
	st.mu.Lock()
	entries := st.entries[start:end]
	st.mu.Unlock()
	return entries
}

// ==================== SoftState ====================

// 保存在内存中的实时状态
type SoftState struct {
	commitIndex int // 已经提交的最大的日志索引，由当前节点维护
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
	st.mu.Lock()
	commitIndex := st.commitIndex
	st.mu.Unlock()
	return commitIndex
}

func (st *SoftState) setCommitIndex(index int) {
	st.mu.Lock()
	st.commitIndex = index
	st.mu.Unlock()
}

func (st *SoftState) setLastApplied(index int) {
	st.mu.Lock()
	st.lastApplied = index
	st.mu.Unlock()
}

func (st *SoftState) lastAppliedAdd() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastApplied += 1
	return st.lastApplied
}

func (st *SoftState) softLastApplied() int {
	st.mu.Lock()
	lastApplied := st.lastApplied
	st.mu.Unlock()
	return lastApplied
}

// ==================== PeerState ====================

// 对等节点状态和路由表
type PeerState struct {
	peers  map[NodeId]NodeAddr // 所有节点
	me     NodeId              // 当前节点在 peers 中的索引
	leader NodeId              // 当前 leader 在 peers 中的索引
	mu     sync.Mutex
}

func newPeerState(peers map[NodeId]NodeAddr, me NodeId) *PeerState {
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

func (st *PeerState) leaderId() NodeId {
	st.mu.Lock()
	leaderId := st.leader
	st.mu.Unlock()
	return leaderId
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

func newLeaderState() *LeaderState {
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

func (st *LeaderState) peerNextIndex(id NodeId) int {
	st.mu.Lock()
	nextIndex := st.nextIndex[id]
	st.mu.Unlock()
	return nextIndex
}

func (st *LeaderState) setNextIndex(id NodeId, index int) {
	st.mu.Lock()
	st.nextIndex[id] = index
	st.mu.Unlock()
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
	aeBusy    map[NodeId]bool // Follower 是否正在忙于 AE 通信
	mu        sync.Mutex      // 修改 aeBusy 字段要加锁

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

func (st *timerState) initTimerState(peers map[NodeId]NodeAddr) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.timer = time.NewTimer(st.electionDuration())

	st.aeBusy = make(map[NodeId]bool, len(peers))
	for id, _ := range peers {
		st.aeBusy[id] = false
	}

	st.timerType = Election
}

func (st *timerState) getTimerType() timerType {
	st.mu.Lock()
	timerT := st.timerType
	st.mu.Unlock()
	return timerT
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

func (st *timerState) setAeState(id NodeId, state bool) {
	st.mu.Lock()
	st.aeBusy[id] = state
	st.mu.Unlock()
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

func (st *timerState) isAeBusy(id NodeId) bool {
	st.mu.Lock()
	isBusy := st.aeBusy[id]
	st.mu.Unlock()
	return isBusy
}

// ==================== snapshotState ====================

type snapshotState struct {
	snapshot     *Snapshot
	persister    SnapshotPersister
	maxLogLength int
	mu           sync.Mutex
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
	st.mu.Lock()
	defer st.mu.Unlock()

	err := st.persister.SaveSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("保存快照失败：%w", err)
	}
	st.snapshot = &snapshot
	return nil
}

func (st *snapshotState) needGenSnapshot(commitIndex int) bool {
	st.mu.Lock()
	need := commitIndex - st.snapshot.LastIndex >= st.maxLogLength
	st.mu.Unlock()
	return need
}

func (st *snapshotState) lastIndex() int {
	st.mu.Lock()
	index := st.snapshot.LastIndex
	st.mu.Unlock()
	return index
}

func (st *snapshotState) getSnapshot() *Snapshot {
	st.mu.Lock()
	data := st.snapshot
	st.mu.Unlock()
	return data
}
