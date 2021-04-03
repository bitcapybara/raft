package raft

type rpcType uint8

// 日志类型
type EntryType uint8

const (
	EntryReplicate EntryType = iota
	EntryChangeConf
	EntryHeartbeat
	EntryTimeoutNow
	EntryPromote
)

// 日志条目
type Entry struct {
	Index int       // 此条目的逻辑索引， 从 1 开始
	Term  int       // 日志项所在term
	Type  EntryType // 日志类型
	Data  []byte    // 状态机命令
}

type Status uint8

const (
	OK Status = iota
	NotLeader
)

type Server struct {
	Id   NodeId
	Addr NodeAddr
}

type NodeId string

const None NodeId = ""

type NodeAddr string

// ==================== AppendEntry ====================

type AppendEntry struct {
	EntryType    EntryType // 载荷的条目类型
	Term         int       // 当前时刻所属任期
	LeaderId     NodeId    // 领导者的地址，方便 Follower 重定向
	PrevLogIndex int       // 要发送的日志条目的前一个条目的索引
	PrevLogTerm  int       // PrevLogIndex 条目所处任期
	LeaderCommit int       // Leader 提交的索引
	Entries      []Entry   // 日志条目
}

type AppendEntryReply struct {
	Term               int  // 当前时刻所属任期，用于领导者更新自身
	ConflictTerm       int  // 当前节点与 Leader 发生冲突的日志的 Term
	ConflictStartIndex int  // 发生冲突的 Term 包含的第一条日志
	Success            bool // 如果关注者包含与prevLogIndex和prevLogTerm匹配的条目，则为true
}

// ==================== RequestVote ====================

type RequestVote struct {
	Term         int    // 当前时刻所属任期
	CandidateId  NodeId // 候选人id
	LastLogIndex int    // 发送此请求的 Candidate 最后一个日志条目的索引
	LastLogTerm  int    // LastLogIndex 所处的任期
}

type RequestVoteReply struct {
	Term        int  // 当前时刻所属任期，用于领导者更新自身
	VoteGranted bool // 为 true 表示候选人收到一个选票
}

// ==================== InstallSnapshot ====================

type InstallSnapshot struct {
	Term              int    // Leader 的当前 Term
	LeaderId          NodeId // Leader 的 nodeId
	LastIncludedIndex int    // 快照要替换的日志条目截止索引
	LastIncludedTerm  int    // LastIncludedIndex 所在位置的条目的 Term
	Offset            int64  // 分批发送数据时，当前块的字节偏移量
	Data              []byte // 快照的序列化数据
	Done              bool   // 分批发送是否完成
}

type InstallSnapshotReply struct {
	Term int // 接收的 Follower 的当前 Term
}

// ==================== ApplyCommand ====================

type ApplyCommand struct {
	Data []byte // 客户端请求应用到状态机的数据
}

type ApplyCommandReply struct {
	Status Status // 客户端请求的是 Leader 节点时，返回 true
	Leader Server // 客户端请求的不是 Leader 节点时，返回 LeaderId
}

// ==================== ChangeConfig ====================

type ChangeConfig struct {
	Peers map[NodeId]NodeAddr // 新配置的集群各节点
}

type ChangeConfigReply struct {
	Status Status // 配置变更结果
}

// ==================== TransferLeadership ====================

type TransferLeadership struct {
	Transferee Server
}

type TransferLeadershipReply struct {
	Status Status
}

// ==================== AddNewNode ====================

type AddNewNode struct {
	NewNode Server
}

type AddNewNodeReply struct {
	Status Status
}
