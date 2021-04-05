package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==================== RoleState ====================

const (
	Learner   RoleStage = iota // 日志同步者
	Follower                   // 追随者
	Candidate                  // 候选者
	Leader                     // 领导者
)

// 角色类型
type RoleStage uint8

func RoleFromString(role string) (roleStage RoleStage) {
	switch role {
	case "Learner":
		roleStage = Learner
	case "Follower":
		roleStage = Follower
	case "Candidate":
		roleStage = Candidate
	case "Leader":
		roleStage = Leader
	}
	return
}

func RoleToString(roleStage RoleStage) (role string) {
	switch roleStage {
	case Learner:
		role = "Learner"
	case Follower:
		role = "Follower"
	case Candidate:
		role = "Candidate"
	case Leader:
		role = "Leader"
	}
	return
}

type RoleState struct {
	roleStage RoleStage  // 节点当前角色
	mu        sync.Mutex // 角色并发访问锁
}

func newRoleState(stage RoleStage) *RoleState {
	return &RoleState{
		roleStage: stage,
	}
}

func (st *RoleState) setRoleStage(stage RoleStage) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.roleStage = stage
}

func (st *RoleState) getRoleStage() RoleStage {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.roleStage
}

// ==================== HardState ====================

// 需要持久化存储的状态
type HardState struct {
	term      int                // 当前时刻所处的 term
	votedFor  NodeId             // 当前任期获得选票的 Candidate
	entries   []Entry            // 当前节点保存的日志
	persister RaftStatePersister // 持久化器
	mu        sync.Mutex
}

func (st *HardState) lastEntryIndex() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	lastLogIndex := len(st.entries) - 1
	if lastLogIndex < 0 {
		return 0
	} else {
		return st.entries[lastLogIndex].Index
	}
}

func (st *HardState) currentTerm() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.term
}

func (st *HardState) logLength() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.entries)
}

func (st *HardState) setTerm(term int) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.term >= term {
		return nil
	}
	err := st.persist(term, "", st.entries)
	if err != nil {
		return fmt.Errorf("持久化出错，设置 Term 属性值失败。%w", err)
	}
	st.term = term
	st.votedFor = ""
	return nil
}

func (st *HardState) termAddAndVote(delta int, voteTo NodeId) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	newTerm := st.term + delta
	err := st.persist(newTerm, voteTo, st.entries)
	if err != nil {
		return fmt.Errorf("持久化出错，设置 Term 属性值失败。%w", err)
	}
	st.term = newTerm
	st.votedFor = voteTo
	return nil
}

func (st *HardState) vote(id NodeId) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.votedFor == id {
		return nil
	}
	err := st.persist(st.term, id, st.entries)
	if err != nil {
		return fmt.Errorf("持久化出错，设置 votedFor 属性值失败。%w", err)
	}
	st.votedFor = id
	return nil
}

func (st *HardState) persist(term int, votedFor NodeId, entries []Entry) error {
	raftState := RaftState{
		Term:     term,
		VotedFor: votedFor,
		Entries:  entries,
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
	err := st.persist(st.term, st.votedFor, append(st.entries[:], entry))
	if err != nil {
		return fmt.Errorf("持久化出错，设置 Entries 属性值失败。%w", err)
	}
	st.entries = append(st.entries, entry)
	return nil
}

func (st *HardState) logEntry(index int) (entry Entry, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if index >= len(st.entries) {
		err = errors.New("索引超出范围！")
	}
	entry = st.entries[index]
	return
}

func (st *HardState) voted() NodeId {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.votedFor
}

func (st *HardState) clearEntries() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.entries = make([]Entry, 0)
}

func (st *HardState) logEntries(start, end int) []Entry {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.entries[start:end]
}

func (st *HardState) truncateAfter(index int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.entries = st.entries[:index]
}

func (st *HardState) truncateBefore(index int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.entries = st.entries[index:]
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

func (st *SoftState) getCommitIndex() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.commitIndex
}

func (st *SoftState) setCommitIndex(index int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.commitIndex = index
}

func (st *SoftState) setLastApplied(index int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastApplied = index
}

func (st *SoftState) lastAppliedAdd() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastApplied += 1
	return st.lastApplied
}

func (st *SoftState) getLastApplied() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.lastApplied
}

// ==================== PeerState ====================

// 对等节点状态和路由表
type PeerState struct {
	peersMap map[NodeId]NodeAddr // 所有节点
	me       NodeId              // 当前节点在 peersMap 中的索引
	leader   NodeId              // 当前 leader 在 peersMap 中的索引
	mu       sync.Mutex
}

func newPeerState(peers map[NodeId]NodeAddr, me NodeId) *PeerState {
	return &PeerState{
		peersMap: peers,
		me:       me,
		leader:   "",
	}
}

func (st *PeerState) leaderIsMe() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.leader == st.me
}

func (st *PeerState) majority() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.peersMap)/2 + 1
}
func (st *PeerState) peers() map[NodeId]NodeAddr {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.peersMap
}

func (st *PeerState) replacePeers(peers map[NodeId]NodeAddr) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.peersMap = peers
}

func (st *PeerState) replacePeersWithBytes(from []byte) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	// 	获取新节点集
	peers, err := decodePeersMap(from)
	if err != nil {
		return err
	}
	st.peersMap = peers
	return nil
}

func decodePeersMap(from []byte) (map[NodeId]NodeAddr, error) {
	var peers map[NodeId]NodeAddr
	decoder := gob.NewDecoder(bytes.NewBuffer(from))
	err := decoder.Decode(&peers)
	if err != nil {
		return nil, err
	} else {
		return peers, nil
	}
}

func (st *PeerState) peersCnt() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.peersMap)
}

func (st *PeerState) isMe(id NodeId) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return id == st.me
}

func (st *PeerState) myId() NodeId {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.me
}

func (st *PeerState) setLeader(id NodeId) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.leader = id
}

func (st *PeerState) leaderId() NodeId {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.leader
}

func (st *PeerState) getLeader() Server {
	st.mu.Lock()
	defer st.mu.Unlock()
	return Server{
		Id:   st.leader,
		Addr: st.peersMap[st.leader],
	}
}

func (st *PeerState) addPeer(id NodeId, addr NodeAddr) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.peersMap[id] = addr
}

// ==================== LeaderState ====================

type trigger uint8

const (
	FindNextIndex trigger = iota
	FindMatchIndex
)

type Replication struct {
	id         NodeId        // 节点标识
	addr       NodeAddr      // 节点地址
	role       RoleStage     // 复制对象的角色
	nextIndex  int           // 下一次要发送给各节点的日志索引。由 Leader 维护，初始值为 Leader 最后一个日志的索引 + 1
	matchIndex int           // 已经复制到各节点的最大的日志索引。由 Leader 维护，初始值为0
	rpcBusy    bool          // 是否正在通信
	mu         sync.Mutex    // 锁
	stepDownCh chan int      // 通知主线程降级
	stopCh     chan struct{} // 接收主线程发来的降级通知
	triggerCh  chan trigger  // 触发复制请求
}

type transfer struct {
	leadTransferee NodeId          // 如果正在进行所有权转移，转移的目标id
	timer          *time.Timer     // 领导权转移超时计时器
	reply          chan<- rpcReply // 领导权转移 rpc 答复
	mu             sync.Mutex
}

func newTransfer() *transfer {
	return &transfer{
		leadTransferee: None,
	}
}

type configChange struct {
	oldConfig map[NodeId]NodeAddr // 旧配置
	newConfig map[NodeId]NodeAddr // 新配置
	mu        sync.Mutex
}

// 节点是 Leader 时，保存在内存中的状态
type LeaderState struct {
	stepDownCh   chan int                // 接收降级通知
	done         chan NodeId             // 日志复制结束
	replications map[NodeId]*Replication // 代表了一个复制日志的 Follower 节点
	transfer     *transfer               // 领导权转移状态
	configChange *configChange           // 配置变更状态
}

func newLeaderState(peers map[NodeId]NodeAddr) *LeaderState {
	stepDownCh := make(chan int)
	replications := make(map[NodeId]*Replication)
	for id, addr := range peers {
		replications[id] = &Replication{
			id:         id,
			addr:       addr,
			role:       Follower,
			nextIndex:  1,
			matchIndex: 0,
			stepDownCh: stepDownCh,
			stopCh:     make(chan struct{}),
			triggerCh:  make(chan trigger),
		}
	}
	return &LeaderState{
		stepDownCh:   stepDownCh,
		done:         make(chan NodeId),
		replications: replications,
		transfer:     newTransfer(),
		configChange: &configChange{},
	}
}

func (st *LeaderState) followers() map[NodeId]*Replication {
	return st.replications
}

func (st *LeaderState) matchIndex(id NodeId) int {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	return st.replications[id].matchIndex
}

func (st *LeaderState) setMatchAndNextIndex(id NodeId, matchIndex, nextIndex int) {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	st.replications[id].matchIndex = matchIndex
	st.replications[id].nextIndex = nextIndex
}

func (st *LeaderState) matchAndNextIndexAdd(id NodeId) {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	st.replications[id].matchIndex++
	st.replications[id].nextIndex++
}

func (st *LeaderState) nextIndex(id NodeId) int {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	return st.replications[id].nextIndex
}

func (st *LeaderState) setNextIndex(id NodeId, index int) {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	st.replications[id].nextIndex = index
}

func (st *LeaderState) setRpcBusy(id NodeId, busy bool) {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	st.replications[id].rpcBusy = busy
}

func (st *LeaderState) isRpcBusy(id NodeId) bool {
	st.replications[id].mu.Lock()
	defer st.replications[id].mu.Unlock()
	return st.replications[id].rpcBusy
}

func (st *LeaderState) setTransferBusy(id NodeId) {
	st.transfer.mu.Lock()
	defer st.transfer.mu.Unlock()
	st.transfer.leadTransferee = id
}

func (st *LeaderState) isTransferBusy() (NodeId, bool) {
	st.transfer.mu.Lock()
	defer st.transfer.mu.Unlock()
	return st.transfer.leadTransferee, st.transfer.leadTransferee != None
}

func (st *LeaderState) setTransferState(timer *time.Timer, reply chan<- rpcReply) {
	st.transfer.mu.Lock()
	defer st.transfer.mu.Unlock()
	st.transfer.timer = timer
	st.transfer.reply = reply
}

func (st *LeaderState) setOldConfig(oldPeers map[NodeId]NodeAddr) {
	st.configChange.mu.Lock()
	defer st.configChange.mu.Unlock()
	st.configChange.oldConfig = oldPeers
}

func (st *LeaderState) setNewConfig(newPeers map[NodeId]NodeAddr) {
	st.configChange.mu.Lock()
	defer st.configChange.mu.Unlock()
	st.configChange.newConfig = newPeers
}

func (st *LeaderState) getOldConfig() map[NodeId]NodeAddr {
	st.configChange.mu.Lock()
	defer st.configChange.mu.Unlock()
	return st.configChange.oldConfig
}

func (st *LeaderState) getNewConfig() map[NodeId]NodeAddr {
	st.configChange.mu.Lock()
	defer st.configChange.mu.Unlock()
	return st.configChange.newConfig
}

func (st *LeaderState) oldMajority() int {
	st.configChange.mu.Lock()
	defer st.configChange.mu.Unlock()
	return len(st.configChange.oldConfig)/2 + 1
}

func (st *LeaderState) newMajority() int {
	st.configChange.mu.Lock()
	defer st.configChange.mu.Unlock()
	return len(st.configChange.newConfig)/2 + 1
}

func (st *LeaderState) roleUpgrade(id NodeId) {
	state := st.replications[id]
	state.mu.Lock()
	defer state.mu.Unlock()
	state.role = Follower
}

// ==================== timerState ====================

type timerState struct {
	timeoutTimer *time.Timer // 超时计时器

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

// 用于计时器已到期后重置
func (st *timerState) setElectionTimer() {
	if st.timeoutTimer == nil {
		st.timeoutTimer = time.NewTimer(st.electionDuration())
	} else {
		st.timeoutTimer.Stop()
		st.timeoutTimer.Reset(st.electionDuration())
	}
}

func (st *timerState) setHeartbeatTimer() {
	if st.timeoutTimer == nil {
		st.timeoutTimer = time.NewTimer(st.heartbeatDuration())
	} else {
		st.timeoutTimer.Stop()
		st.timeoutTimer.Reset(st.heartbeatDuration())
	}
}

func (st *timerState) electionDuration() time.Duration {
	randTimeout := rand.Intn(st.electionMaxTimeout-st.electionMinTimeout) + st.electionMinTimeout
	return time.Millisecond * time.Duration(randTimeout)
}

func (st *timerState) minElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(st.electionMinTimeout)
}

func (st *timerState) heartbeatDuration() time.Duration {
	return time.Millisecond * time.Duration(st.heartbeatTimeout)
}

func (st *timerState) tick() <-chan time.Time {
	return st.timeoutTimer.C
}

// ==================== snapshotState ====================

type snapshotState struct {
	snapshot     *Snapshot
	persister    SnapshotPersister
	maxLogLength int
	mu           sync.Mutex
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

func (st *snapshotState) logThreshold() int {
	return st.maxLogLength
}

func (st *snapshotState) lastIndex() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.snapshot.LastIndex
}

func (st *snapshotState) lastTerm() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.snapshot.LastTerm
}

func (st *snapshotState) getSnapshot() *Snapshot {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.snapshot
}
