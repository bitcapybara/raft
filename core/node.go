package core

import (
	"github.com/smallnest/rpcx/v6/server"
	"log"
)

// 构造 Node 对象时的配置参数
type Config struct {
	Fsm                *Fsm
	Persister          *Persister
	peers              map[NodeId]NodeAddr
	me                 NodeId
	ElectionMinTimeout int
	ElectionMaxTimeout int
	HeartbeatTimeout   int
}

// 代表了一个当前节点
type Node struct {
	raft   *raft  // 节点所具有的 raft 功能对象
	config Config // 节点配置对象
	timer  *timer // 计时器
}

func NewNode(config Config) *Node {
	return &Node{
		raft:   NewRaft(config),
		config: config,
		timer: NewTimer(config),
	}
}

func (nd *Node) Run() {
	// 开启 rpc 服务器
	go nd.rpcServer()

	// 初始化定时器
	nd.timer.initTimer()

	// 监听并处理计时器事件
	for {
		select {
		case <-nd.timer.electionTimer.C:
			// 选举计时器到期
			if !nd.raft.isLeader() {
				// 开始新选举
			}
			// 重置选举计时器
			nd.timer.setElectionTimer()
		case <-nd.timer.heartbeatTimer.C:
			// 心跳计时器到期
			if nd.raft.isLeader() || nd.timer.isEnable() {
				// 发送心跳
			}
			nd.timer.setHeartbeatTimer()
		}
	}
}

func (nd *Node) rpcServer() {
	s := server.NewServer()
	err := s.RegisterName("Node", nd, "")
	if err != nil {
		log.Fatalf("rpc 服务器注册服务失败：%s\n", err)
	}
	err = s.Serve("tcp", string(nd.config.peers[nd.config.me]))
	log.Fatalf("rpc 服务器开启监听失败：%s\n", err)
}


// Follower 和 Candidate 开放的 rpc接口，由 Leader 调用
func (nd *Node) AppendEntries(args AppendEntry, res *AppendEntryReply) error {
	// 重置选举计时器
	nd.timer.resetElectionTimer()

	return nil
}

// Follower 和 Candidate 开放的 rpc 接口，由 Candidate 调用
func (nd *Node) RequestVote(args RequestVote, res *RequestVoteReply) error {
	return nil
}

// Follower 开放的 rpc 接口，由 Leader 调用
func (nd *Node) InstallSnapshot(args InstallSnapshot, res InstallSnapshotReply) error {
	return nil
}

// 客户端调用此方法来应用日志条目
func (nd *Node) Apply(data []byte) error {
	return nil
}
