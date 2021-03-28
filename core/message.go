package core

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

type status uint8

const (
	OK status = iota
	NotLeader
	Timeout  // todo 请求超时
)

type server struct {
	id   NodeId
	addr NodeAddr
}

type NodeId string

const None NodeId = ""

type NodeAddr string

// ==================== AppendEntry ====================

type AppendEntry struct {
	entryType    EntryType // 载荷的条目类型
	term         int       // 当前时刻所属任期
	leaderId     NodeId    // 领导者的地址，方便 Follower 重定向
	prevLogIndex int       // 要发送的日志条目的前一个条目的索引
	prevLogTerm  int       // prevLogIndex 条目所处任期
	leaderCommit int       // Leader 提交的索引
	entries      []Entry   // 日志条目（心跳为空；todo 为提高效率可能发送多个）
}

type AppendEntryReply struct {
	term               int  // 当前时刻所属任期，用于领导者更新自身
	conflictTerm       int  // 当前节点与 Leader 发生冲突的日志的 term
	conflictStartIndex int  // 发生冲突的 term 包含的第一条日志
	success            bool // 如果关注者包含与prevLogIndex和prevLogTerm匹配的条目，则为true
}

// ==================== RequestVote ====================

type RequestVote struct {
	term         int    // 当前时刻所属任期
	candidateId  NodeId // 候选人id
	lastLogIndex int    // 发送此请求的 Candidate 最后一个日志条目的索引
	lastLogTerm  int    // lastLogIndex 所处的任期
}

type RequestVoteReply struct {
	term        int  // 当前时刻所属任期，用于领导者更新自身
	voteGranted bool // 为 true 表示候选人收到一个选票
}

// ==================== InstallSnapshot ====================

type InstallSnapshot struct {
	term              int    // Leader 的当前 term
	leaderId          NodeId // Leader 的 nodeId
	lastIncludedIndex int    // 快照要替换的日志条目截止索引
	lastIncludedTerm  int    // lastIncludedIndex 所在位置的条目的 term
	offset            int64  // 分批发送数据时，当前块的字节偏移量
	data              []byte // 快照的序列化数据
	done              bool   // 分批发送是否完成
}

type InstallSnapshotReply struct {
	term int // 接收的 Follower 的当前 term
}

// ==================== ApplyCommand ====================

type ApplyCommand struct {
	data []byte // 客户端请求应用到状态机的数据
}

type ApplyCommandReply struct {
	status status // 客户端请求的是 Leader 节点时，返回 true
	leader server // 客户端请求的不是 Leader 节点时，返回 LeaderId
}

// ==================== ChangeConfig ====================

type ChangeConfig struct {
	peers map[NodeId]NodeAddr // 新配置的集群各节点
}

type ChangeConfigReply struct {
	status status // 配置变更结果
}

// ==================== TransferLeadership ====================

type TransferLeadership struct {
	transferee server
}

type TransferLeadershipReply struct {
	status status
}

// ==================== AddNewNode ====================

type AddNewNode struct {
	newNode server
}

type AddNewNodeReply struct {
	status status
}
