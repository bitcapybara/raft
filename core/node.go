package core

import (
	"github.com/smallnest/rpcx/v6/server"
	"log"
)

// 构造 Node 对象时的配置参数
type Config struct {
	Fsm                Fsm
	RaftStatePersister RaftStatePersister
	SnapshotPersister  SnapshotPersister
	Peers              map[NodeId]NodeAddr
	Me                 NodeId
	ElectionMinTimeout int
	ElectionMaxTimeout int
	HeartbeatTimeout   int
}

// 代表了一个当前节点
type Node struct {
	raft         *raft         // 节点所具有的 raft 功能对象
	config       Config        // 节点配置对象
	timerManager *timerManager // 计时器
}

func NewNode(config Config) *Node {
	return &Node{
		raft:         NewRaft(config),
		config:       config,
		timerManager: NewTimerManager(config),
	}
}

func (nd *Node) Run() {
	// 开启 rpc 服务器
	go nd.rpcServer()

	// 初始化定时器
	nd.timerManager.initTimerManager(nd.raft.peers())

	// 监听并处理计时器事件
	nd.startTimer()
}

func (nd *Node) startTimer() {
	// 监听并处理心跳计时器事件
	peerTimerMap := nd.timerManager.heartbeatTimer
	for id, tmr := range peerTimerMap {
		// Leader 为每个 Follower 开启一个心跳计时器
		go func(id NodeId, tmr *timer) {
			for {
				select {
				case <-tmr.timer.C:
					if nd.raft.isLeader() || tmr.isEnable() {
						// 发送心跳
						nd.raft.heartbeat(id)
					}
					nd.timerManager.setHeartbeatTimer(id)
				}
			}
		}(id, tmr)
	}

	// 监听并处理选举计时器事件
	go func() {
		for {
			select {
			case <-nd.timerManager.electionTimer.C:
				// 选举计时器到期
				if !nd.raft.isLeader() {
					// 开始新选举
					nd.raft.election()
				}
				// 重置选举计时器
				nd.timerManager.setElectionTimer()
			}
		}
	}()
}

func (nd *Node) rpcServer() {
	s := server.NewServer()
	err := s.RegisterName("Node", nd, "")
	if err != nil {
		log.Fatalf("rpc 服务器注册服务失败：%s\n", err)
	}
	err = s.Serve("tcp", string(nd.config.Peers[nd.config.Me]))
	log.Fatalf("rpc 服务器开启监听失败：%s\n", err)
}

// Follower 和 Candidate 开放的 rpc接口，由 Leader 调用
func (nd *Node) AppendEntries(args AppendEntry, res *AppendEntryReply) error {
	// 重置选举计时器
	nd.timerManager.resetElectionTimer()
	return nd.raft.handleCommand(args, res)
}

// Follower 和 Candidate 开放的 rpc 接口，由 Candidate 调用
func (nd *Node) RequestVote(args RequestVote, res *RequestVoteReply) error {
	return nd.raft.handleVoteReq(args, res)
}

// Follower 开放的 rpc 接口，由 Leader 调用
func (nd *Node) InstallSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {
	return nd.raft.handleSnapshot(args, res)
}

// Leader 开放的 rpc 接口，由客户端调用
func (nd *Node) ClientApply(args ClientRequest, res *ClientResponse) error {
	return nd.raft.handleClientReq(args, res)
}
