package core

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

// 构造 Node 对象时的配置参数
type Config struct {
	Fsm                Fsm
	RaftStatePersister RaftStatePersister
	SnapshotPersister  SnapshotPersister
	Transport          Transport
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
}

func NewNode(config Config) *Node {
	return &Node{
		raft:         newRaft(config),
		config:       config,
	}
}

func (nd *Node) Run() {
	// 监听并处理计时器事件
	nd.raft.raftRun()
}

// Follower 和 Candidate 开放的 rpc接口，由 Leader 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) AppendEntries(args AppendEntry, res *AppendEntryReply) error {
	// 重置选举计时器
	nd.raft.resetElectionTimer()
	return nd.raft.handleCommand(args, res)
}

// Follower 和 Candidate 开放的 rpc 接口，由 Candidate 调用
// 客户端接收到请求后，调用此方法
func (nd *Node) RequestVote(args RequestVote, res *RequestVoteReply) error {
	err := nd.raft.handleVoteReq(args, res)
	if err != nil {
		return err
	}
	if res.voteGranted {
		nd.raft.resetElectionTimer()
	}
	return nil
}

// Follower 开放的 rpc 接口，由 Leader 调用
// 客户端接收到请求后，调用此方法
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
	var wg sync.WaitGroup
	for id, addr := range nd.raft.peers() {
		wg.Add(1)
		go func(id NodeId, addr NodeAddr) {
			defer func() {
				wg.Done()
				nd.raft.setAeState(id, false)
			}()
			// Follower 节点忙于 AE 请求，不需要发送心跳
			nd.raft.setAeState(id, true)
			// 给节点发送日志条目
			err = nd.raft.sendLogEntry(id, addr)
			if err != nil {
				log.Println(err)
			} else {
				atomic.AddInt32(&successCnt, 1)
			}
		}(id, addr)
	}

	wg.Wait()
	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	if int(successCnt) < nd.raft.majority() {
		return fmt.Errorf("日志条目没有复制到多数节点")
	}

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目
	err = nd.raft.updateLeaderCommitIndex()
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
