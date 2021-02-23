package core

import (
	"github.com/go-errors/errors"
	"github.com/smallnest/rpcx/v6/server"
	"log"
	"sync/atomic"
	"time"
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
	MaxLogLength       int
}

// 代表了一个当前节点
type Node struct {
	raft         *raft         // 节点所具有的 raft 功能对象
	config       Config        // 节点配置对象
	timerManager *timerManager // 计时器
}

func NewNode(config Config) *Node {
	return &Node{
		raft:         newRaft(config),
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
		if nd.raft.isMe(id) {
			continue
		}
		go func(id NodeId, tmr *time.Timer) {
			for {
				// 等待心跳计时器到期
				<-tmr.C

				if nd.raft.isLeader() {
					// 发送心跳
					nd.raft.heartbeat(id)
				}
				// 重置心跳计时器
				nd.timerManager.setHeartbeatTimer(id)
			}
		}(id, tmr)
	}

	// 监听并处理选举计时器事件
	go func() {
		for {
			// 等待选举计时器到期
			<-nd.timerManager.electionTimer.C

			// 重置选举计时器
			nd.timerManager.setElectionTimer()

			if !nd.raft.isLeader() {
				// 开始新选举
				nd.raft.election()
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
	// todo 向另一个节点投票后，重置选举计时器
	return nd.raft.handleVoteReq(args, res)
}

// Follower 开放的 rpc 接口，由 Leader 调用
func (nd *Node) InstallSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {
	return nd.raft.handleSnapshot(args, res)
}

// Leader 开放的 rpc 接口，由客户端调用
func (nd *Node) ClientApply(args ClientRequest, res *ClientResponse) error {
	if !nd.raft.isLeader() {
		res.ok = false
		res.leaderId = nd.raft.leaderId()
		return nil
	}

	// 构造需要复制的日志
	err := nd.raft.appendEntry(Entry{Term: nd.raft.term(), Data: args.data})
	if err != nil {
		log.Println(err)
	}

	// 给各节点发送日志条目
	var successCnt int32 = 0
	for id, addr := range nd.raft.peers() {
		go func(id NodeId, addr NodeAddr) {
			nd.timerManager.stopHeartbeatTimer(id)
			// 给节点发送日志条目
			err := nd.raft.sendLogEntry(id, addr)
			if err != nil {
				log.Println(err)
			} else {
				atomic.AddInt32(&successCnt, 1)
			}
			nd.timerManager.setHeartbeatTimer(id)
		}(id, addr)
	}

	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	if int(successCnt) < nd.raft.majority() {
		return errors.New("日志条目没有复制到多数节点")
	}

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目
	err = nd.raft.setCommitIndex(nd.raft.lastLogIndex())
	if err != nil {
		log.Println(err)
	}


	// 当日志量超过阈值时，生成快照
	snapshot, err := nd.raft.persister.LoadSnapshot()
	if err != nil {
		log.Println(err)
	}
	// 快照数据发送给所有 Follower 节点
	if nd.raft.commitIndex()-snapshot.LastIndex <= nd.config.MaxLogLength {
		return nil
	}
	bytes, err := nd.raft.fsm.Serialize()
	if err != nil {
		return nil
	}

	for id, addr := range nd.raft.peers() {
		go func(id NodeId, addr NodeAddr) {
			err := nd.raft.sendSnapshot(id, addr, bytes)
			if err != nil {
				log.Println(err)
			}
		}(id, addr)
	}
	return nil
}
