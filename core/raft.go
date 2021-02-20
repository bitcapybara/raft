package core

import (
	"context"
	"github.com/smallnest/rpcx/v6/client"
	"log"
	"sync"
	"sync/atomic"
)

type raft struct {
	roleState   *RoleState        // 当前节点的角色
	persister   SnapshotPersister // 快照生成器
	fsm         Fsm               // 客户端状态机
	hardState   *HardState        // 需要持久化存储的状态
	softState   *SoftState        // 保存在内存中的实时状态
	peerState   *PeerState        // 对等节点状态和路由表
	leaderState *LeaderState      // 节点是 Leader 时，保存在内存中的状态
	mu          sync.Mutex
}

func NewRaft(config Config) *raft {
	raftPst := config.RaftStatePersister
	snptPst := config.SnapshotPersister
	raftState, err := raftPst.LoadRaftState()
	var hardState HardState
	if err != nil {
		log.Println(err)
		hardState = NewHardState(raftPst)
	} else {
		hardState = raftState.toHardState(raftPst)
	}

	return &raft{
		roleState:   NewRoleState(),
		persister:   snptPst,
		fsm:         config.Fsm,
		hardState:   &hardState,
		softState:   NewSoftState(),
		peerState:   NewPeerState(config.Peers, config.Me),
		leaderState: NewLeaderState(),
	}
}

func (rf *raft) isLeader() bool {
	roleStage := rf.roleState.getRoleStage()
	leaderIsMe := rf.peerState.leaderIsMe()
	return roleStage == Leader && leaderIsMe
}

func (rf *raft) roleStage() RoleStage {
	return rf.roleState.getRoleStage()
}

func (rf *raft) commitIndex() int {
	return rf.softState.softCommitIndex()
}

func (rf *raft) lastLogIndex() int {
	return rf.hardState.lastLogIndex()
}

func (rf *raft) logLength() int {
	return rf.hardState.logLength()
}

func (rf *raft) matchIndex(id NodeId) int {
	return rf.leaderState.peerMatchIndex(id)
}

func (rf *raft) term() int {
	return rf.hardState.currentTerm()
}

func (rf *raft) majority() int {
	return rf.peerState.majority()
}

func (rf *raft) peers() map[NodeId]NodeAddr {
	return rf.peerState.peersMap()
}

func (rf *raft) isMe(id NodeId) bool {
	return rf.peerState.isMe(id)
}

func (rf *raft) me() NodeId {
	return rf.peerState.identity()
}

func (rf *raft) leaderId() NodeId {
	return rf.peerState.leaderId()
}

func (rf *raft) logEntryTerm(index int) int {
	return rf.hardState.logEntryTerm(index)
}

func (rf *raft) logEntry(index int) Entry {
	return rf.hardState.logEntry(index)
}

func (rf *raft) votedFor() NodeId {
	return rf.hardState.getVotedFor()
}

func (rf *raft) setRoleStage(stage RoleStage) {
	rf.roleState.setRoleStage(stage)
}

func (rf *raft) vote(id NodeId) error {
	return rf.hardState.vote(id)
}

func (rf *raft) setLeader(id NodeId) {
	rf.peerState.setLeader(id)
}

func (rf *raft) setMatchAndNextIndex(id NodeId, matchIndex, nextIndex int) {
	rf.leaderState.setMatchAndNextIndex(id, matchIndex, nextIndex)
}

func (rf *raft) setLastApplied(index int) {
	rf.softState.setLastApplied(index)
}

func (rf *raft) lastApplied() int {
	return rf.softState.softLastApplied()
}

func (rf *raft) truncateEntries(index int) {
	rf.hardState.truncateEntries(index)
}

func (rf *raft) clearEntries() {
	rf.hardState.clearEntries()
}

// 降级为 Follower
func (rf *raft) degrade(term int) error {
	if rf.roleState.getRoleStage() == Follower {
		return nil
	}
	// 更新状态
	rf.roleState.setRoleStage(Follower)
	return rf.hardState.setTerm(term)
}

// 更新自身 term
func (rf *raft) setTerm(term int) error {
	if rf.term() == term {
		return nil
	}
	// 更新状态
	return rf.hardState.setTerm(term)
}

func (rf *raft) appendEntry(entry Entry) error {
	return rf.hardState.appendEntry(entry)
}

func (rf *raft) setCommitIndex(index int) {
	rf.softState.setCommitIndex(index)
}

func (rf *raft) applyFsm(applyIndex int) error {
	prevCommit := rf.commitIndex()
	if applyIndex > prevCommit {
		if applyIndex >= rf.lastLogIndex() {
			rf.setCommitIndex(rf.lastLogIndex())
		} else {
			rf.setCommitIndex(applyIndex)
		}
	}

	commitIndex := rf.commitIndex()
	if prevCommit != rf.commitIndex() {
		logEntry := rf.logEntry(commitIndex)
		err := rf.fsm.Apply(logEntry.Data)
		if err != nil {
			return err
		}
		rf.setLastApplied(rf.lastApplied() + 1)
	}

	return nil
}

// Leader 给某个节点发送心跳
func (rf *raft) heartbeat(id NodeId) {
	addr := rf.peers()[id]
	commitIndex := rf.commitIndex()
	if rf.isMe(id) {
		return
	}
	d, err := client.NewPeer2PeerDiscovery("tcp@"+string(addr), "")
	if err != nil {
		log.Printf("创建rpc客户端失败：%s%s\n", addr, err)
		return
	}
	xClient := client.NewXClient("Node", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	term := rf.term()
	args := AppendEntry{
		term:         term,
		leaderId:     rf.me(),
		prevLogIndex: commitIndex,
		prevLogTerm:  rf.logEntryTerm(commitIndex),
		entries:      nil,
		leaderCommit: commitIndex,
	}
	res := &AppendEntryReply{}
	err = xClient.Call(context.Background(), "AppendEntries", args, res)
	if err != nil {
		log.Printf("调用rpc服务失败：%s%s\n", addr, err)
		return
	}
	err = xClient.Close()
	if err != nil {
		log.Printf("关闭rpc客户端失败：%s\n", err)
	}
	if res.term > term {
		// 当前任期数落后，降级为 Follower
		// todo 使用 context 停掉当前还在运行的协程
		err := rf.degrade(res.term)
		if err != nil {
			log.Println(err)
		}
	}
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.setRoleStage(Candidate)
	// 发送投票请求
	var vote int32 = 0
	err := rf.setTerm(rf.term() + 1)
	if err != nil {
		log.Println(err)
	}
	var wg sync.WaitGroup
	for id, addr := range rf.peers() {
		wg.Add(1)
		go func(id NodeId, addr NodeAddr) {
			defer wg.Done()
			if rf.isMe(id) {
				// 投票给自己
				atomic.AddInt32(&vote, 1)
				vote += 1
				err := rf.vote(rf.me())
				if err != nil {
					log.Println(err)
				}
				return
			}
			d, err := client.NewPeer2PeerDiscovery("tcp@"+string(addr), "")
			if err != nil {
				log.Printf("创建rpc客户端失败：%s%s\n", addr, err)
				return
			}
			xClient := client.NewXClient("Node", client.Failtry, client.RandomSelect, d, client.DefaultOption)
			args := RequestVote{
				term:        rf.term(),
				candidateId: id,
			}
			res := &RequestVoteReply{}
			err = xClient.Call(context.Background(), "AppendEntries", args, res)
			if err != nil {
				log.Printf("调用rpc服务失败：%s%s\n", addr, err)
				return
			}
			err = xClient.Close()
			if err != nil {
				log.Printf("关闭rpc客户端失败：%s\n", err)
			}
			if res.term <= rf.term() && res.voteGranted {
				// 成功获得选票
				atomic.AddInt32(&vote, 1)
			} else if res.term > rf.term() {
				// 当前节点任期落后，则退出竞选
				// todo 使用 context 停掉当前还在运行的协程
				err := rf.degrade(res.term)
				if err != nil {
					log.Println(err)
				}
			}
		}(id, addr)
	}
	wg.Wait()
	if rf.roleStage() == Candidate && int(vote) > rf.majority() {
		// 获得了大多数选票，转换为 Leader
		rf.setRoleStage(Leader)
		rf.setLeader(rf.me())
		for id := range rf.peers() {
			rf.setMatchAndNextIndex(id, 0, rf.lastLogIndex() + 1)
		}
	}
}

// Follower 和 Candidate 接收到来自 Leader 的 AppendEntries 调用
func (rf *raft) handleCommand(args AppendEntry, res *AppendEntryReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rfTerm := rf.term()
	if args.term < rfTerm {
		// 发送请求的 Leader 任期数落后
		res.term = rfTerm
		res.success = false
		return nil
	}

	// ========== 接收日志条目 ==========
	if len(args.entries) != 0 {
		prevIndex := args.prevLogIndex

		if prevIndex >= rf.logLength() || rf.logEntryTerm(prevIndex) != args.prevLogTerm {
			res.term = rfTerm
			res.success = false
			return nil
		}
		// 将新条目添加到日志中，如果已经存在，则覆盖
		err := rf.appendEntry(args.entries[0])
		if err != nil {
			log.Println(err)
		}
		// 提交日志条目，并应用到状态机
		return rf.applyFsm(args.leaderCommit)
	}

	// ========== 接收心跳 ==========
	// 任期数落后或相等
	if rf.roleStage() == Candidate {
		// 如果是候选者，需要降级
		rf.setRoleStage(Follower)
	}
	err := rf.setTerm(args.term)
	if err != nil {
		log.Println(err)
	}

	rf.setLeader(args.leaderId)
	res.term = rf.term()
	res.success = true

	// 提交日志条目，并应用到状态机
	return nil
}

// Follower 和 Candidate 接收到来自 Candidate 的 RequestVote 调用
func (rf *raft) handleVoteReq(args RequestVote, res *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	argsTerm := args.term

	rfTerm := rf.term()
	if argsTerm < rfTerm {
		// 拉票的候选者任期落后，不投票
		res.term = rfTerm
		res.voteGranted = false
		return nil
	}

	if argsTerm > rfTerm {
		err := rf.setTerm(argsTerm)
		if err != nil {
			log.Println(err)
		}
		if rf.roleStage() == Candidate {
			// 当前节点是候选者，自动降级
			rf.setRoleStage(Follower)
		}
	}

	res.term = argsTerm
	res.voteGranted = false
	votedFor := rf.votedFor()
	if votedFor == "" || votedFor == args.candidateId {
		// 当前节点是追随者且没有投过票，则投出第一票
		lastIndex := rf.lastLogIndex()
		lastTerm := rf.logEntryTerm(lastIndex)
		if args.lastLogTerm > lastTerm || (args.lastLogTerm == lastTerm && args.lastLogIndex >= lastIndex) {
			err := rf.vote(args.candidateId)
			if err != nil {
				log.Println(err)
			}
			res.voteGranted = true
		}
	}

	return nil
}

// Follower 接收来自 Leader 的 InstallSnapshot 调用
func (rf *raft) handleSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rfTerm := rf.term()
	if args.term < rfTerm {
		// Leader 的 term 过期，直接返回
		res.term = rfTerm
		return nil
	}

	// 持久化
	res.term = rfTerm
	snapshot := Snapshot{args.lastIncludedIndex, args.lastIncludedTerm, args.data}
	err := rf.persister.SaveSnapshot(snapshot)
	if err != nil {
		return err
	}

	if !args.done {
		// 若传送没有完成，则继续接收数据
		return nil
	}

	// 保存快照成功，删除多余日志
	if args.lastIncludedIndex <= rf.logLength() && rf.logEntryTerm(args.lastIncludedIndex) == args.lastIncludedTerm {
		rf.truncateEntries(args.lastIncludedIndex)
		return nil
	}

	rf.clearEntries()
	return nil
}

// 接收来自客户端的 rpc 请求

func (rf *raft) handleClientReq(args ClientRequest, res *ClientResponse) error {
	if !rf.isLeader() {
		res.ok = false
		res.leaderId = rf.leaderId()
		return nil
	}
	return nil
}
