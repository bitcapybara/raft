package core

import (
	"context"
	"github.com/go-errors/errors"
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
}

func newRaft(config Config) *raft {
	raftPst := config.RaftStatePersister
	snptPst := config.SnapshotPersister

	var raftState RaftState
	if raftPst != nil {
		rfState, err := raftPst.LoadRaftState()
		if err != nil {
			log.Println(err)
		} else {
			raftState = rfState
		}
	} else {
		raftState = newRaftState()
	}
	hardState := raftState.toHardState(raftPst)

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

// 降级为 Follower
func (rf *raft) degrade(term int) error {
	if rf.roleState.getRoleStage() == Follower {
		return nil
	}
	// 更新状态
	rf.roleState.setRoleStage(Follower)
	return rf.hardState.setTerm(term)
}

// ==================== RoleState ====================

func (rf *raft) roleStage() RoleStage {
	return rf.roleState.getRoleStage()
}

func (rf *raft) setRoleStage(stage RoleStage) {
	rf.roleState.setRoleStage(stage)
}

// ==================== HardState ====================

func (rf *raft) lastLogIndex() int {
	return rf.hardState.lastLogIndex()
}

func (rf *raft) logLength() int {
	return rf.hardState.logLength()
}

func (rf *raft) term() int {
	return rf.hardState.currentTerm()
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

func (rf *raft) vote(id NodeId) error {
	return rf.hardState.vote(id)
}

func (rf *raft) truncateEntries(index int) {
	rf.hardState.truncateEntries(index)
}

func (rf *raft) clearEntries() {
	rf.hardState.clearEntries()
}

func (rf *raft) entries(start, end int) []Entry {
	return rf.hardState.logEntries(start, end)
}

// 更新自身 term
func (rf *raft) setTerm(term int) error {
	return rf.hardState.setTerm(term)
}

func (rf *raft) appendEntry(entry Entry) error {
	return rf.hardState.appendEntry(entry)
}

// ==================== SoftState ====================

func (rf *raft) commitIndex() int {
	return rf.softState.softCommitIndex()
}

func (rf *raft) setLastApplied(index int) {
	rf.softState.setLastApplied(index)
}

func (rf *raft) lastApplied() int {
	return rf.softState.softLastApplied()
}

func (rf *raft) setCommitIndex(index int) {
	rf.softState.setCommitIndex(index)
}

// ==================== PeerState ====================

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

func (rf *raft) setLeader(id NodeId) {
	rf.peerState.setLeader(id)
}

// ==================== LeaderState ====================

func (rf *raft) matchIndex(id NodeId) int {
	return rf.leaderState.peerMatchIndex(id)
}

func (rf *raft) setMatchAndNextIndex(id NodeId, matchIndex, nextIndex int) {
	rf.leaderState.setMatchAndNextIndex(id, matchIndex, nextIndex)
}

func (rf *raft) nextIndex(id NodeId) int {
	return rf.leaderState.peerNextIndex(id)
}

func (rf *raft) setNextIndex(id NodeId, index int) {
	rf.leaderState.setNextIndex(id, index)
}

// ==================== logic process ====================

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
	xClient := client.NewXClient("Node", client.Failfast, client.RandomSelect, d, client.DefaultOption)
	defer func() {
		if err := xClient.Close(); err != nil {
			log.Println("关闭客户端失败")
		}
	}()
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

	if res.term > term {
		// 当前任期数落后，降级为 Follower
		err := rf.degrade(res.term)
		if err != nil {
			log.Println(err)
		}
	}

	// Follower 和 Leader 的日志不匹配，则进行日志同步
	if !res.success || rf.nextIndex(id) != rf.lastLogIndex() + 1 {
		err := rf.sendLogEntry(id, addr)
		if err != nil {
			log.Println(err)
		}
	}
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election() {
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
	rfTerm := rf.term()
	if args.term < rfTerm {
		// 发送请求的 Leader 任期数落后
		res.term = rfTerm
		res.success = false
		return nil
	}

	// 处理请求
	prevIndex := args.prevLogIndex
	if prevIndex >= rf.logLength() || rf.logEntryTerm(prevIndex) != args.prevLogTerm {
		res.term = rfTerm
		res.success = false
		return nil
	}

	// ========== 接收日志条目 ==========
	if len(args.entries) != 0 {

		// 如果当前节点已经有此条目但冲突
		if rf.lastLogIndex() >= prevIndex + 1 && (rf.logEntryTerm(prevIndex+1) != args.term) {
			rf.truncateEntries(prevIndex+1)
		}

		// 将新条目添加到日志中，如果已经存在，则覆盖
		err := rf.appendEntry(args.entries[0])
		if err != nil {
			log.Println(err)
		}
		return nil
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

// 给指定的节点发送日志条目
func (rf *raft) sendLogEntry(id NodeId, addr NodeAddr) error {

	// 不用给自己发
	if rf.isMe(id) {
		return nil
	}
	// 创建 rpc 客户端
	d, err := client.NewPeer2PeerDiscovery("tcp@"+string(addr), "")
	if err != nil {
		log.Printf("创建rpc客户端失败：%s%s\n", addr, err)
		return nil
	}
	xClient := client.NewXClient("Node", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer func() {
		if err := xClient.Close(); err != nil {
			log.Println("关闭客户端失败")
		}
	}()

	var prevIndex int
	// Follower 节点中必须存在第 prevIndex 条日志，才能接受第 nextIndex 条日志
	// 如果 Follower 节点缺少很多日志，需要找到缺少的第一条日志的索引
	for {
		prevIndex = rf.nextIndex(id) - 1
		args := AppendEntry{
			term:         rf.term(),
			leaderId:     rf.me(),
			prevLogIndex: prevIndex,
			prevLogTerm:  rf.logEntryTerm(prevIndex),
			leaderCommit: rf.commitIndex(),
			entries:      rf.entries(prevIndex, rf.nextIndex(id)),
		}
		res := &AppendEntryReply{}
		err = xClient.Call(context.Background(), "AppendEntries", args, res)
		if err != nil {
			log.Printf("调用rpc服务失败：%s%s\n", addr, err)
			continue
		}
		if res.term > rf.term() {
			// 如果任期数小，降级为 Follower
			err := rf.degrade(res.term)
			if err != nil {
				log.Println(err)
			}
			break
		}
		if res.success {
			break
		}

		// 向前继续查找 Follower 缺少的第一条日志的索引
		rf.setNextIndex(id, rf.nextIndex(id)-1)
	}

	// 给 Follower 发送缺失的日志，发送的日志的索引一直递增到最新
	snapshot, err := rf.persister.LoadSnapshot()
	if err != nil {
		log.Println(err)
	}
	for prevIndex != rf.lastLogIndex() {
		// 缺失的日志太多时，直接发送快照
		if rf.nextIndex(id) <= snapshot.LastIndex {
			err := rf.sendSnapshot(id, addr, snapshot.Data)
			if err != nil {
				log.Println(err)
			} else {
				rf.setMatchAndNextIndex(id, snapshot.LastIndex, snapshot.LastIndex + 1)
			}
		}

		prevIndex = rf.nextIndex(id) - 1
		args := AppendEntry{
			term:         rf.term(),
			leaderId:     rf.me(),
			prevLogIndex: prevIndex,
			prevLogTerm:  rf.logEntryTerm(prevIndex),
			leaderCommit: rf.commitIndex(),
			entries:      rf.entries(prevIndex, rf.nextIndex(id)),
		}
		res := &AppendEntryReply{}
		err = xClient.Call(context.Background(), "AppendEntries", args, res)
		if err != nil {
			log.Printf("调用rpc服务失败：%s%s\n", addr, err)
			continue
		}
		if res.term > rf.term() {
			// 如果任期数小，降级为 Follower
			err := rf.degrade(res.term)
			if err != nil {
				log.Println(err)
			}
			break
		}

		// 向后补充
		rf.setMatchAndNextIndex(id, rf.nextIndex(id), rf.nextIndex(id)+1)
	}

	return nil
}

func (rf *raft) sendSnapshot(id NodeId, addr NodeAddr, data []byte) error {
	if rf.isMe(id) {
		// 自己保存快照
		newSnapshot := Snapshot{
			LastIndex: rf.commitIndex(),
			LastTerm: rf.term(),
			Data: data,
		}
		return rf.persister.SaveSnapshot(newSnapshot)
	}
	d, err := client.NewPeer2PeerDiscovery("tcp@"+string(addr), "")
	if err != nil {
		return errors.Errorf("创建rpc客户端失败：%s%s\n", addr, err)
	}
	xClient := client.NewXClient("Node", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer func() {
		if err := xClient.Close(); err != nil {
			log.Println("关闭客户端失败")
		}
	}()
	args := InstallSnapshot{
		term: rf.term(),
		leaderId: rf.me(),
		lastIncludedIndex: rf.commitIndex(),
		lastIncludedTerm: rf.logEntryTerm(rf.commitIndex()),
		offset: 0,
		data: data,
		done: true,
	}
	res := &InstallSnapshotReply{}
	err = xClient.Call(context.Background(), "AppendEntries", args, res)
	if err != nil {
		return errors.Errorf("调用rpc服务失败：%s%s\n", addr, err)
	}
	if res.term > rf.term() {
		// 如果任期数小，降级为 Follower
		return rf.degrade(res.term)
	}

	return nil
}
