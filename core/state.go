package core

import "sync"

// ==================== RoleState ====================

const (
	Leader    RoleStage = iota // 领导者
	Candidate                  // 候选者
	Follower                   // 追随者
)

const (
	ElectTimeout   RoleEvent = iota // 选举计时器超时
	MajorityVotes                   // 获得大多数节点选票
	DiscoverLeader                  // 接收到 Leader 心跳
	GetHigherTerm                   // 获取到更高的 term 数
)

// 角色类型
type RoleStage uint8

// 角色转换事件类型
type RoleEvent uint8

type RoleState struct {
	roleStage   RoleStage      // 节点当前角色
	mu sync.RWMutex
}

func NewRoleState() *RoleState {
	return &RoleState{
		roleStage:   Follower,
	}
}

func (rs *RoleState) setRoleStage(event RoleEvent) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	switch event {
	case ElectTimeout:
		if rs.roleStage == Follower {
			rs.roleStage = Candidate
			return true
		}
	case MajorityVotes:
		if rs.roleStage == Candidate {
			rs.roleStage = Leader
			return true
		}
	case DiscoverLeader:
		if rs.roleStage == Candidate {
			rs.roleStage = Follower
			return true
		}
	case GetHigherTerm:
		if rs.roleStage != Follower {
			rs.roleStage = Follower
			return true
		}
	}

	return false
}

func (rs *RoleState) getRoleStage() RoleStage {
	rs.mu.RLock()
	stage := rs.roleStage
	rs.mu.RUnlock()
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

	// 当前时刻所处的 term
	term int

	// 当前任期获得选票的 Candidate
	// 由 Follower 维护，当前节点 term 值改变时重置为空
	votedFor NodeId

	// 当前节点保存的日志
	entries []Entry

	mu sync.RWMutex
}

func NewHardState() *HardState {
	return &HardState{
		term: 1,
		votedFor: "",
		entries: []Entry{},
	}
}

// ==================== SoftState ====================

// 保存在内存中的实时状态
type SoftState struct {

	// 已经提交的最大的日志索引，由当前节点维护。
	commitIndex int

	// 应用到状态机的最后一个日志索引
	lastApplied int

	mu sync.RWMutex
}

func NewSoftState() *SoftState {
	return &SoftState{
		commitIndex: 0,
		lastApplied: 0,
	}
}

// ==================== PeerState ====================

// 对等节点状态和路由表
type PeerState struct {

	// 所有节点
	peers map[NodeId]NodeAddr

	// 当前节点在 peers 中的索引
	me NodeId

	// 当前 leader 在 peers 中的索引
	leader NodeId

	mu sync.RWMutex
}

func NewPeerState(peers map[NodeId]NodeAddr, me NodeId) *PeerState {
	return &PeerState{
		peers: peers,
		me: me,
		leader: "",
	}
}

// ==================== LeaderState ====================

// 节点是 Leader 时，保存在内存中的状态
type LeaderState struct {

	// 下一次要发送给各节点的日志索引。由 Leader 维护，初始值为 Leader 最后一个日志的索引 + 1
	nextIndex map[NodeId]int

	// 已经复制到各节点的最大的日志索引。由 Leader 维护，初始值为0
	matchIndex map[NodeId]int

	mu sync.RWMutex
}

func NewLeaderState() *LeaderState {
	return &LeaderState{
		nextIndex: make(map[NodeId]int),
		matchIndex: make(map[NodeId]int),
	}
}
