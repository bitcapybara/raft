package core

// 代表了一个当前节点
type Node struct {
	raft   *raft
	config Config // 节点配置对象
}

func NewNode(config Config) *Node {
	return &Node{
		raft:   newRaft(config),
		config: config,
	}
}

func (nd *Node) Run() {
	// 监听并处理计时器事件
	nd.raft.raftRun()
}

// Follower 和 Candidate 开放的 rpc接口，由 Leader 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) AppendEntries(args AppendEntry, res *AppendEntryReply) error {
	return nd.raft.handleCommand(args, res)
}

// Follower 和 Candidate 开放的 rpc 接口，由 Candidate 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) RequestVote(args RequestVote, res *RequestVoteReply) error {
	return nd.raft.handleVoteReq(args, res)
}

// Follower 开放的 rpc 接口，由 Leader 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) InstallSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {
	return nd.raft.handleSnapshot(args, res)
}

// Leader 开放的 rpc 接口，由客户端调用
func (nd *Node) ApplyCommand(args ClientRequest, res *ClientResponse) error {
	return nd.raft.handleClientCmd(args, res)
}

// Leader 开放的 rpc 接口，由客户端调用，添加新节点
func (nd *Node) AddServer(args AddServer, res *AddServerReply) error {
	return nd.raft.handleServerAdd(args, res)
}

// Leader 开放的 rpc 接口，由客户端调用，移除旧节点
func (nd *Node) RemoveServer(args RemoveServer, res *RemoveServerReply) error {
	return nd.raft.handleServerRemove(args, res)
}
