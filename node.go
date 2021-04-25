package raft

const (
	// 来自 Leader 的日志复制请求
	AppendEntryRpc rpcType = iota
	// 来自 Candidate 的投票请求
	RequestVoteRpc
	// 来自 Leader 的安装快照请求
	InstallSnapshotRpc
	// 来自客户端的安装命令请求
	ApplyCommandRpc
	// 来自客户端的成员变更请求
	ChangeConfigRpc
	// 来自客户端的领导权转移请求
	TransferLeadershipRpc
	// 来自客户端的添加 Learner 节点请求
	AddLearnerRpc
)

type rpc struct {
	rpcType rpcType
	req     interface{}
	res     chan rpcReply
}

type rpcReply struct {
	res interface{}
	err error
}

// 代表了一个当前节点
type Node struct {
	raft   *raft
	config Config // 节点配置对象
	rpcCh  chan rpc
}

func NewNode(config Config) *Node {
	return &Node{
		raft:   newRaft(config),
		config: config,
		rpcCh:  make(chan rpc),
	}
}

func (nd *Node) Run() {
	// 开启 raft 循环
	nd.raft.raftRun(nd.rpcCh)
}

// 判断当前节点是否是 Leader 节点
func (nd *Node) IsLeader() bool {
	return nd.raft.isLeader()
}

// Follower 和 Candidate 开放的 rpc接口，由 Leader 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) AppendEntries(args AppendEntry, res *AppendEntryReply) error {
	if msg := nd.sendRpc(AppendEntryRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(AppendEntryReply)
		return nil
	}
}

// Follower 和 Candidate 开放的 rpc 接口，由 Candidate 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) RequestVote(args RequestVote, res *RequestVoteReply) error {
	if msg := nd.sendRpc(RequestVoteRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(RequestVoteReply)
		return nil
	}
}

// Follower 开放的 rpc 接口，由 Leader 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) InstallSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {
	if msg := nd.sendRpc(InstallSnapshotRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(InstallSnapshotReply)
		return nil
	}
}

// Leader 开放的 rpc 接口，由客户端调用
func (nd *Node) ApplyCommand(args ApplyCommand, res *ApplyCommandReply) error {
	if msg := nd.sendRpc(ApplyCommandRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(ApplyCommandReply)
		return nil
	}
}

// Leader 开放的 rpc 接口，由客户端调用，添加新配置
func (nd *Node) ChangeConfig(args ChangeConfig, res *ChangeConfigReply) error {
	if msg := nd.sendRpc(ChangeConfigRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(ChangeConfigReply)
		return nil
	}
}

// Leader 开放的 rpc 接口，由客户端调用，转移领导权
func (nd *Node) TransferLeadership(args TransferLeadership, res *TransferLeadershipReply) error {
	if msg := nd.sendRpc(TransferLeadershipRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(TransferLeadershipReply)
		return nil
	}
}

// Leader 开放的 rpc 接口，由客户端调用，添加新的 Learner 节点
func (nd *Node) AddLearner(args AddLearner, res *AddLearnerReply) error {
	if msg := nd.sendRpc(AddLearnerRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(AddLearnerReply)
		return nil
	}
}

func (nd *Node) sendRpc(rpcType rpcType, args interface{}) rpcReply {
	rpcMsg := rpc{
		rpcType: rpcType,
		req: args,
		res: make(chan rpcReply),
	}
	nd.rpcCh <- rpcMsg
	return <- rpcMsg.res
}
