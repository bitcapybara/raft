package core

// 代表了一个当前节点
type Node struct {
	raft   *raft
	config Config // 节点配置对象
	rpcCh chan rpc
}

func NewNode(config Config) *Node {
	return &Node{
		raft:   newRaft(config),
		config: config,
		rpcCh: make(chan rpc),
	}
}

func (nd *Node) Run() {
	// 开启 raft 循环
	nd.raft.raftRun(nd.rpcCh)
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
func (nd *Node) ApplyCommand(args ClientApply, res *ClientApplyReply) error {
	if msg := nd.sendRpc(ClientApplyRpc, args); msg.err != nil {
		return msg.err
	} else {
		*res = msg.res.(ClientApplyReply)
		return nil
	}
}

// Leader 开放的 rpc 接口，由客户端调用，添加新节点
func (nd *Node) AddServer(args AddServer, res *AddServerReply) error {
	return nd.raft.handleServerAdd(args, res)
}

// Leader 开放的 rpc 接口，由客户端调用，移除旧节点
func (nd *Node) RemoveServer(args RemoveServer, res *RemoveServerReply) error {
	return nd.raft.handleServerRemove(args, res)
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
