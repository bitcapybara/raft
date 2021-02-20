package core

import (
	"context"
	"github.com/smallnest/rpcx/v6/client"
	"github.com/smallnest/rpcx/v6/server"
	"log"
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
		if nd.raft.isMe(id) {
			continue
		}
		go func(id NodeId, tmr *time.Timer) {
			for {
				select {
				case <-tmr.C:
					if nd.raft.isLeader() {
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
	if !nd.raft.isLeader() {
		res.ok = false
		res.leaderId = nd.raft.leaderId()
		return nil
	}

	// 日志复制
	newEntry := Entry{
		Index: nd.raft.commitIndex() + 1,
		Term:  nd.raft.term(),
		Data:  args.data,
	}

	err := nd.raft.appendEntry(newEntry)
	if err != nil {
		log.Println(err)
	}

	successCnt := 0
	for id, addr := range nd.raft.peers() {
		nd.timerManager.stopHeartbeatTimer(id)
		err := nd.raft.handleClientReq(id, addr)
		if err != nil {
			log.Println(err)
		} else {
			successCnt += 1
		}
		nd.timerManager.setHeartbeatTimer(id)
	}

	if successCnt > nd.raft.majority() {
		// 新日志成功发送到过半 Follower 节点
		nd.raft.setCommitIndex(nd.raft.commitIndex()+1)
		err := nd.raft.applyFsm(nd.raft.lastLogIndex())
		if err != nil {
			log.Println(err)
		}
		// 当日志量超过阈值时，生成快照
		snapshot, err := nd.raft.persister.LoadSnapshot()
		if err != nil {
			log.Println(err)
		}
		if nd.raft.commitIndex() - snapshot.LastIndex > 1000 {
			bytes, err := nd.raft.fsm.Serialize()
			if err != nil {
				return nil
			}

			// 快照数据发送给所有 Follower 节点
			for id, addr := range nd.raft.peers() {
				if nd.raft.isMe(id) {
					// 自己保存快照
					newSnapshot := Snapshot{
						LastIndex: nd.raft.commitIndex(),
						LastTerm: nd.raft.term(),
						Data: bytes,
					}
					err := nd.raft.persister.SaveSnapshot(newSnapshot)
					if err != nil {
						log.Println(err)
					}
					continue
				}
				d, err := client.NewPeer2PeerDiscovery("tcp@"+string(addr), "")
				if err != nil {
					log.Printf("创建rpc客户端失败：%s%s\n", addr, err)
					continue
				}
				xClient := client.NewXClient("Node", client.Failtry, client.RandomSelect, d, client.DefaultOption)
				err = xClient.Call(context.Background(), "AppendEntries", args, res)
				if err != nil {
					log.Printf("调用rpc服务失败：%s%s\n", addr, err)
					continue
				}
				args := InstallSnapshot{
					term: nd.raft.term(),
					leaderId: nd.raft.me(),
					lastIncludedIndex: nd.raft.commitIndex(),
					lastIncludedTerm: nd.raft.logEntryTerm(nd.raft.commitIndex()),
					offset: 0,
					done: true,
				}
				res := &InstallSnapshotReply{}
				err = xClient.Call(context.Background(), "AppendEntries", args, res)
				if err != nil {
					log.Printf("调用rpc服务失败：%s%s\n", addr, err)
					continue
				}
				err = xClient.Close()
				if err != nil {
					log.Println(err)
				}
				if res.term > nd.raft.term() {
					// 如果任期数小，降级为 Follower
					err := nd.raft.degrade(res.term)
					if err != nil {
						log.Println(err)
					}
					break
				}
			}
		}
	}
	return nil
}
