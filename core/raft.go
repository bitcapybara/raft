package core

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type finishMsg uint8

const (
	Noop finishMsg = iota
	Success
	RpcFailed
	Degrade
)

type raft struct {
	roleState     *RoleState     // 当前节点的角色
	fsm           Fsm            // 客户端状态机
	transport     Transport      // 发送请求的接口
	hardState     *HardState     // 需要持久化存储的状态
	softState     *SoftState     // 保存在内存中的实时状态
	peerState     *PeerState     // 对等节点状态和路由表
	leaderState   *LeaderState   // 节点是 Leader 时，保存在内存中的状态
	timerState    *timerState    // 计时器状态
	snapshotState *snapshotState // 快照状态

	// 所有方法都加锁，保证同一时间 raft 只接收一个改变状态的请求
	mu sync.Mutex
}

func newRaft(config Config) *raft {
	raftPst := config.RaftStatePersister

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
		roleState:     newRoleState(),
		fsm:           config.Fsm,
		transport:     config.Transport,
		hardState:     &hardState,
		softState:     newSoftState(),
		peerState:     newPeerState(config.Peers, config.Me),
		leaderState:   newLeaderState(),
		timerState:    newTimerState(config),
		snapshotState: newSnapshotState(config),
	}
}

func (rf *raft) raftRun() {
	// 初始化定时器
	rf.timerState.initTimerState()
	go func() {
		tm := rf.timerState
		for {
			<-tm.timer.C
			if rf.isHeartbeatTick() {
				rf.heartbeat()
			} else if rf.isElectionTick() {
				rf.election()
			}
		}
	}()
}

func (rf *raft) isHeartbeatTick() bool {
	return rf.timerState.getTimerType() == Heartbeat && rf.isLeader()
}

func (rf *raft) isElectionTick() bool {
	return rf.timerState.getTimerType() == Election && !rf.isLeader()
}

// ==================== logic process ====================

func (rf *raft) heartbeat() {

	// 重置心跳计时器
	rf.timerState.setHeartbeatTimer()

	finishCh := make(chan finishMsg)
	defer close(finishCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for id := range rf.peerState.peersMap() {
		if rf.peerState.isMe(id) || rf.leaderState.isRpcBusy(id) {
			continue
		}
		go rf.replicationTo(ctx, id, finishCh, false)
	}

	rf.waitReplication(finishCh)
}

// Leader 给某个节点发送心跳/日志
func (rf *raft) replicationTo(ctx context.Context, id NodeId, msgCh chan finishMsg, withEntry bool) {
	msg := Noop
	rf.leaderState.setRpcBusy(id, true)
	defer func() {
		rf.leaderState.setRpcBusy(id, false)
		if msg != Noop {
			msgCh <- msg
		}
	}()

	addr := rf.peerState.peersMap()[id]
	prevIndex := rf.leaderState.peerNextIndex(id) - 1
	var entries []Entry
	if withEntry {
		entries = rf.hardState.logEntries(rf.hardState.lastEntryIndex(), rf.hardState.logLength())
	}
	args := AppendEntry{
		term:         rf.hardState.currentTerm(),
		leaderId:     rf.peerState.myId(),
		prevLogIndex: prevIndex,
		prevLogTerm:  rf.logTerm(prevIndex),
		entries:      entries,
		leaderCommit: rf.softState.softCommitIndex(),
	}
	res := &AppendEntryReply{}
	err := rf.transport.AppendEntries(addr, args, res)

	if err != nil {
		log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
		msg = RpcFailed
		return
	}

	if res.success {
		msg = Success
		return
	}

	if res.term > rf.hardState.currentTerm() {
		// 当前任期数落后，降级为 Follower
		if degradeErr := rf.degrade(res.term); degradeErr != nil {
			log.Println(err)
			msg = Success
		} else {
			msg = Degrade
		}
		return
	} else {
		// Follower 和 Leader 的日志不匹配，进行日志追赶
		go rf.appendPeerEntry(id, addr)
	}

	select {
	case <- ctx.Done():
		msg = Noop
	default:
		break
	}
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election() {
	// 重置选举计时器
	rf.timerState.setElectionTimer()

	// 增加 term 数
	err := rf.hardState.setTerm(rf.hardState.currentTerm() + 1)
	if err != nil {
		log.Println(err)
	}
	// 角色置为候选者
	rf.setRoleStage(Candidate)
	// 投票给自己
	err = rf.hardState.vote(rf.peerState.myId())
	if err != nil {
		log.Println(err)
	}
	// 发送 RV 请求
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for id, addr := range rf.peerState.peersMap() {
		if rf.peerState.isMe(id) {
			continue
		}
		currentTerm := rf.hardState.currentTerm()
		go func(id NodeId, addr NodeAddr, term int) {

			var msg finishMsg
			rf.leaderState.setRpcBusy(id, true)
			defer func() {
				rf.leaderState.setRpcBusy(id, false)
				if msg != Noop {
					finishCh <- msg
				}
			}()

			args := RequestVote{
				term:        currentTerm,
				candidateId: id,
			}
			res := &RequestVoteReply{}
			rpcErr := rf.transport.RequestVote(addr, args, res)


			if rpcErr != nil {
				log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
				msg  = RpcFailed
				return
			}

			if res.voteGranted {
				// 成功获得选票
				msg = Success
				return
			}

			if res.term > rf.hardState.currentTerm() {
				// 当前任期数落后，降级为 Follower
				if degradeErr := rf.degrade(res.term); degradeErr != nil {
					log.Println(degradeErr)
					msg = Success
				} else {
					msg = Degrade
				}
				return
			}

			select {
			case <- ctx.Done():
				msg = Noop
			default:
				break
			}
		}(id, addr, currentTerm)
	}

	count := 1
	voteCnt := 1
	for msg := range finishCh {
		if msg == Success {
			voteCnt += 1
			if voteCnt >= rf.peerState.majority() {
				rf.becomeLeader()
				break
			}
		}
		count += 1
		if count >= rf.peerState.peersCnt() {
			break
		}
	}
}

func (rf *raft) becomeLeader() {
	rf.timerState.setHeartbeatTimer()
	rf.setRoleStage(Leader)
	// 初始化保存的各节点状态
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for id := range rf.peerState.peersMap() {
		if rf.peerState.isMe(id) {
			continue
		}
		rf.leaderState.setMatchAndNextIndex(id, 0, rf.lastLogIndex()+1)
		rf.leaderState.initNotifier(id)
		go rf.replicationTo(ctx, id, finishCh, false)
	}

	rf.waitReplication(finishCh)
}

// msgCh 日志复制协程 -> 主协程，通知协程的任务完成
func (rf *raft) waitReplication(finishCh chan finishMsg) int {
	count := 1
	successCnt := 1
	for msg := range finishCh {
		if msg == Degrade {
			break
		}
		if msg == Success {
			successCnt += 1
		}
		if successCnt >= rf.peerState.majority() {
			break
		}
		count += 1
		if count >= rf.peerState.peersCnt() {
			break
		}
	}

	return successCnt
}

// Follower 和 Candidate 接收到来自 Leader 的 AppendEntries 调用
func (rf *raft) handleCommand(args AppendEntry, res *AppendEntryReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 重置选举计时器
	rf.timerState.resetElectionTimer()

	// 判断 term
	rfTerm := rf.hardState.currentTerm()
	if args.term < rfTerm {
		// 发送请求的 Leader 任期数落后
		res.term = rfTerm
		res.success = false
		return nil
	}

	// 日志一致性检查
	prevIndex := args.prevLogIndex
	if prevIndex > rf.lastLogIndex() {
		// 当前节点不包含索引为 prevIndex 的日志
		// 返回最后一个日志条目的 term 及此 term 的首个条目的索引
		logLength := rf.hardState.logLength()
		if logLength <= 0 {
			res.conflictStartIndex = rf.snapshotState.lastIndex()
			res.conflictTerm = rf.snapshotState.lastTerm()
		} else {
			res.conflictTerm = rf.logTerm(logLength - 1)
			res.conflictStartIndex = rf.hardState.lastEntryIndex()
			for i := logLength - 1; i >= 0 && rf.logTerm(i) == res.conflictTerm; i-- {
				res.conflictStartIndex = rf.hardState.logEntry(i).Index
			}
		}
		res.term = rfTerm
		res.success = false
		return nil
	}
	prevTerm := rf.logTerm(prevIndex)
	if prevTerm != args.prevLogTerm {
		// 节点包含索引为 prevIndex 的日志但是 term 数不同
		// 返回 prevIndex 所在 term 及此 term 的首个条目的索引
		res.conflictTerm = prevTerm
		res.conflictStartIndex = prevIndex
		for i := prevIndex - 1; i >= 0 && rf.logTerm(i) == res.conflictTerm; i-- {
			res.conflictStartIndex = rf.hardState.logEntry(i).Index
		}
		res.term = rfTerm
		res.success = false
		return nil
	}

	// 任期数落后或相等
	if rf.roleState.getRoleStage() == Candidate {
		// 如果是候选者，需要降级
		err := rf.degrade(args.term)
		if err != nil {
			log.Println(err)
		}
	}

	newEntryIndex := prevIndex + 1
	if len(args.entries) != 0 {
		// ========== 接收日志条目 ==========
		// 如果当前节点已经有此条目但冲突
		if rf.lastLogIndex() >= newEntryIndex && rf.logTerm(newEntryIndex) != args.term {
			rf.hardState.truncateEntries(prevIndex + 1)
		}

		// 将新条目添加到日志中
		err := rf.addEntry(args.entries[0])
		if err != nil {
			log.Println(err)
		}
		// 添加日志后不提交，下次心跳来了再提交
		return nil
	}

	// ========== 接收心跳 ==========
	rf.peerState.setLeader(args.leaderId)
	res.term = rf.hardState.currentTerm()
	res.success = true

	// 更新提交索引
	leaderCommit := args.leaderCommit
	if leaderCommit > rf.softState.softCommitIndex() {
		var err error
		if leaderCommit >= newEntryIndex {
			rf.softState.setCommitIndex(newEntryIndex)
			err = rf.applyFsm()
		} else {
			rf.softState.setCommitIndex(leaderCommit)
			err = rf.applyFsm()
		}
		return err
	}

	return nil
}

// Follower 和 Candidate 接收到来自 Candidate 的 RequestVote 调用
func (rf *raft) handleVoteReq(args RequestVote, res *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var err error
	argsTerm := args.term
	rfTerm := rf.hardState.currentTerm()
	if argsTerm < rfTerm {
		// 拉票的候选者任期落后，不投票
		res.term = rfTerm
		res.voteGranted = false
		return nil
	}

	if argsTerm > rfTerm {
		// 角色降级
		if rf.roleState.getRoleStage() != Follower {
			err = rf.degrade(argsTerm)
		} else {
			err = rf.hardState.setTerm(argsTerm)
		}
		if err != nil {
			err = fmt.Errorf("角色降级失败：%w", err)
		}
	}

	res.term = argsTerm
	res.voteGranted = false
	votedFor := rf.hardState.voted()
	if votedFor == "" || votedFor == args.candidateId {
		// 当前节点是追随者且没有投过票
		lastIndex := rf.lastLogIndex()
		lastTerm := rf.logTerm(lastIndex)
		// 候选者的日志比当前节点的日志要新，则投票
		// 先比较 term，term 相同则比较日志长度
		if args.lastLogTerm > lastTerm || (args.lastLogTerm == lastTerm && args.lastLogIndex >= lastIndex) {
			err = rf.hardState.vote(args.candidateId)
			if err != nil {
				err = fmt.Errorf("")
			}
			res.voteGranted = true
		}
	}

	if res.voteGranted {
		rf.timerState.resetElectionTimer()
	}

	return err
}

// Follower 接收来自 Leader 的 InstallSnapshot 调用
func (rf *raft) handleSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rfTerm := rf.hardState.currentTerm()
	if args.term < rfTerm {
		// Leader 的 term 过期，直接返回
		res.term = rfTerm
		return nil
	}

	// 持久化
	res.term = rfTerm
	snapshot := Snapshot{args.lastIncludedIndex, args.lastIncludedTerm, args.data}
	err := rf.snapshotState.save(snapshot)
	if err != nil {
		return err
	}

	if !args.done {
		// 若传送没有完成，则继续接收数据
		return nil
	}

	// 保存快照成功，删除多余日志
	if args.lastIncludedIndex <= rf.hardState.logLength() && rf.logTerm(args.lastIncludedIndex) == args.lastIncludedTerm {
		rf.hardState.truncateEntries(args.lastIncludedIndex)
		return nil
	}

	rf.hardState.clearEntries()
	return nil
}

// 给各节点发送客户端日志
func (rf *raft) handleClientCmd(args ClientRequest, res *ClientResponse) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 重置心跳计时器
	rf.timerState.setHeartbeatTimer()

	if !rf.isLeader() {
		res.status = NotLeader
		res.leader = rf.peerState.getLeader()
		return nil
	}

	res.status = OK

	// Leader 先将日志添加到内存
	err := rf.addEntry(Entry{Term: rf.hardState.currentTerm(), Data: args.data})
	if err != nil {
		log.Println(err)
	}

	// 给各节点发送日志条目
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	for id := range rf.peerState.peersMap() {
		// 不用给自己发
		if rf.peerState.isMe(id) {
			continue
		}
		// 发送日志
		go rf.replicationTo(ctx, id, finishCh, true)
	}

	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	successCnt := rf.waitReplication(finishCh)
	cancel()
	if successCnt < rf.peerState.majority() {
		return fmt.Errorf("rpc 完成，但日志未复制到多数节点")
	}

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目并应用到状态季节
	err = rf.updateLeaderCommit()
	if err != nil {
		log.Println(err)
	}

	// 当日志量超过阈值时，生成快照
	if !rf.snapshotState.needGenSnapshot(rf.softState.softCommitIndex()) {
		return nil
	}
	bytes, err := rf.fsm.Serialize()
	if err != nil {
		return nil
	}

	// 快照数据发送给所有 Follower 节点
	for id, addr := range rf.peerState.peersMap() {
		go func(id NodeId, addr NodeAddr) {
			err = rf.sendSnapshot(id, addr, bytes)
			if err != nil {
				log.Println(err)
			}
		}(id, addr)
	}
	return nil
}

// 给指定节点发送最新日志
// 若日志不同步，开始进行日志追赶操作
// 1. Follower 节点标记为日志追赶状态，下一次心跳时跳过此节点
// 2. 日志追赶完毕或 rpc 调用失败，Follower 节点标记为普通状态
func (rf *raft) appendPeerEntry(id NodeId, addr NodeAddr) {
	// 向前查找 nextIndex 值
	rf.findCorrectNextIndex(id, addr)

	// 递增更新 matchIndex 值
	rf.completeEntries(id, addr)
}

func (rf *raft) findCorrectNextIndex(id NodeId, addr NodeAddr) {
	rl := rf.leaderState
	peerNextIndex := rl.peerNextIndex(id)

	for peerNextIndex >= 0 {
		prevIndex := rl.peerNextIndex(id) - 1

		// 找到匹配点之前，发送空日志节省带宽
		var entries []Entry
		if rl.peerMatchIndex(id) == prevIndex {
			rf.hardState.logEntries(prevIndex, prevIndex+1)
		}
		args := AppendEntry{
			term:         rf.hardState.currentTerm(),
			leaderId:     rf.peerState.myId(),
			prevLogIndex: prevIndex,
			prevLogTerm:  rf.logTerm(prevIndex),
			leaderCommit: rf.softState.softCommitIndex(),
			entries:      entries,
		}
		res := &AppendEntryReply{}
		err := rf.transport.AppendEntries(addr, args, res)

		if err != nil {
			log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
			return
		}
		if res.term > rf.hardState.currentTerm() {
			// 如果任期数小，降级为 Follower
			err = rf.degrade(res.term)
			if err != nil {
				log.Println(err)
			}
			return
		}
		if res.success {
			return
		}

		conflictStartIndex := res.conflictStartIndex
		// Follower 日志是空的，则 nextIndex 置为 1
		if conflictStartIndex <= 0 {
			conflictStartIndex = 1
		}
		// conflictStartIndex 处的日志是一致的，则 nextIndex 置为下一个
		if rf.logTerm(conflictStartIndex) == res.conflictTerm {
			conflictStartIndex += 1
		}

		// 向前继续查找 Follower 缺少的第一条日志的索引
		rl.setNextIndex(id, conflictStartIndex)
	}
}

func (rf *raft) completeEntries(id NodeId, addr NodeAddr) {
	var err error
	rl := rf.leaderState
	for {
		if rl.peerNextIndex(id)-1 == rf.lastLogIndex() {
			return
		}
		// 缺失的日志太多时，直接发送快照
		snapshot := rf.snapshotState.getSnapshot()
		if rl.peerNextIndex(id) <= snapshot.LastIndex {
			err = rf.sendSnapshot(id, addr, snapshot.Data)
			if err != nil {
				log.Println(err)
			} else {
				rf.leaderState.setMatchAndNextIndex(id, snapshot.LastIndex, snapshot.LastIndex+1)
			}
		}

		prevIndex := rl.peerNextIndex(id) - 1
		args := AppendEntry{
			term:         rf.hardState.currentTerm(),
			leaderId:     rf.peerState.myId(),
			prevLogIndex: prevIndex,
			prevLogTerm:  rf.logTerm(prevIndex),
			leaderCommit: rf.softState.softCommitIndex(),
			entries:      rf.hardState.logEntries(prevIndex, rl.peerNextIndex(id)),
		}
		res := &AppendEntryReply{}
		err = rf.transport.AppendEntries(addr, args, res)

		if err != nil {
			log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
			return
		}
		if res.term > rf.hardState.currentTerm() {
			// 如果任期数小，降级为 Follower
			err = rf.degrade(res.term)
			if err != nil {
				log.Println(err)
			}
			return
		}

		// 向后补充
		rf.leaderState.setMatchAndNextIndex(id, rl.peerNextIndex(id), rl.peerNextIndex(id)+1)
	}
}

func (rf *raft) sendSnapshot(id NodeId, addr NodeAddr, data []byte) error {
	if rf.peerState.isMe(id) {
		// 自己保存快照
		newSnapshot := Snapshot{
			LastIndex: rf.softState.softCommitIndex(),
			LastTerm:  rf.hardState.currentTerm(),
			Data:      data,
		}
		return rf.snapshotState.save(newSnapshot)
	}
	args := InstallSnapshot{
		term:              rf.hardState.currentTerm(),
		leaderId:          rf.peerState.myId(),
		lastIncludedIndex: rf.softState.softCommitIndex(),
		lastIncludedTerm:  rf.logTerm(rf.softState.softCommitIndex()),
		offset:            0,
		data:              data,
		done:              true,
	}
	res := &InstallSnapshotReply{}
	err := rf.transport.InstallSnapshot(addr, args, res)
	if err != nil {
		return fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err)
	}
	if res.term > rf.hardState.currentTerm() {
		// 如果任期数小，降级为 Follower
		return rf.degrade(res.term)
	}

	return nil
}

func (rf *raft) handleServerAdd(args AddServer, res *AddServerReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		res.status = NotLeader
		res.leader = rf.peerState.getLeader()
		return nil
	}

	return nil
}

func (rf *raft) handleServerRemove(args RemoveServer, res *RemoveServerReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		res.status = NotLeader
		res.leader = rf.peerState.getLeader()
		return nil
	}

	return nil
}

// 当前节点是不是 Leader
func (rf *raft) isLeader() bool {
	roleStage := rf.roleState.getRoleStage()
	leaderIsMe := rf.peerState.leaderIsMe()
	isLeader := roleStage == Leader && leaderIsMe
	return isLeader
}

// 降级为 Follower
// 返回结果：(是否需要降级, 降级过程报错)
func (rf *raft) degrade(term int) error {
	rf.roleState.lock()
	defer rf.roleState.unlock()
	if rf.roleState.getRoleStage() == Follower || term <= rf.hardState.currentTerm() {
		return nil
	}
	// 更新状态
	err := rf.hardState.setTerm(term)
	if err != nil {
		return fmt.Errorf("降级失败%w", err)
	}
	rf.setRoleStage(Follower)
	return nil
}

func (rf *raft) setRoleStage(stage RoleStage) {
	rf.roleState.setRoleStage(stage)
	if stage == Leader {
		rf.peerState.setLeader(rf.peerState.myId())
		rf.timerState.resetHeartbeatTimer()
	} else {
		rf.timerState.resetElectionTimer()
	}
}

// 添加新日志
func (rf *raft) addEntry(entry Entry) error {
	index := 1
	lastLogIndex := rf.lastLogIndex()
	lastSnapshotIndex := rf.snapshotState.lastIndex()
	if lastLogIndex <= 0 {
		if lastSnapshotIndex <= 0 {
			entry.Index = index
		} else {
			entry.Index = lastSnapshotIndex
		}
	} else {
		entry.Index = lastLogIndex
	}
	return rf.hardState.appendEntry(entry)
}

// 把日志应用到状态机
func (rf *raft) applyFsm() error {
	commitIndex := rf.softState.softCommitIndex()
	lastApplied := rf.softState.softLastApplied()

	for commitIndex > lastApplied {
		entry := rf.hardState.logEntry(lastApplied + 1)
		err := rf.fsm.Apply(entry.Data)
		if err != nil {
			return fmt.Errorf("应用状态机失败：%w", err)
		}
		lastApplied = rf.softState.lastAppliedAdd()
	}

	return nil
}

// 更新 Leader 的提交索引
func (rf *raft) updateLeaderCommit() error {
	indexCnt := make(map[int]int)
	peers := rf.peerState.peersMap()
	//
	for id := range peers {
		indexCnt[rf.leaderState.peerMatchIndex(id)] = 1
	}

	// 计算出多少个节点有相同的 matchIndex 值
	for index, _ := range indexCnt {
		for index2, cnt2 := range indexCnt {
			if index > index2 {
				indexCnt[index2] = cnt2 + 1
			}
		}
	}

	// 找出超过半数的 matchIndex 值
	maxMajorityMatch := 0
	for index, cnt := range indexCnt {
		if cnt >= rf.peerState.majority() && index > maxMajorityMatch {
			maxMajorityMatch = index
		}
	}

	if rf.softState.softCommitIndex() < maxMajorityMatch {
		rf.softState.setCommitIndex(maxMajorityMatch)
		return rf.applyFsm()
	}

	return nil
}

// 获取最后一个日志条目的逻辑索引
func (rf *raft) lastLogIndex() int {
	index := rf.hardState.lastEntryIndex()
	if index <= 0 {
		index = rf.snapshotState.lastIndex()
	}
	return index
}

// 传入的是逻辑索引
func (rf *raft) logTerm(index int) int {
	realIndex := index - rf.snapshotState.lastIndex() - 1
	if realIndex < 0 {
		return 0
	} else {
		return rf.logTerm(realIndex)
	}
}
