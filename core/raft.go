package core

import (
	"context"
	"fmt"
	"log"
)

type finishMsgType uint8

const (
	Success finishMsgType = iota
	RpcFailed
	Degrade
)

type finishMsg struct {
	msgType finishMsgType
	term    int
}

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

	rpcCh chan rpc // 主线程接收 rpc 消息
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
	go func() {
		for {
			switch rf.roleState.getRoleStage() {
			case Leader:
				rf.runLeader()
			case Candidate:
				rf.runCandidate()
			case Follower:
				rf.runFollower()
			}
		}
	}()
}

func (rf *raft) runLeader() {
	// 初始化心跳定时器
	rf.timerState.setHeartbeatTimer()
	for {
		select {
		case <- rf.timerState.tick():
			// 定时器到期
			rf.heartbeat()
		case rpcMsg := <- rf.rpcCh:
			switch rpcMsg.rpcType {
			case RequestVoteRpc:
			case ClientApplyRpc:
			}
		}
	}
}

func (rf *raft) runCandidate() {
	// 初始化选举计时器
	rf.timerState.setElectionTimer()
	for {
		select {
		case <- rf.timerState.tick():
			// 定时器到期
			rf.election()
		case rpcMsg := <- rf.rpcCh:
			switch rpcMsg.rpcType {
			case AppendEntryRpc:
			case RequestVoteRpc:
			}
		}
	}
}

func (rf *raft) runFollower() {
	// 初始化选举计时器
	rf.timerState.setElectionTimer()
	for {
		select {
		case <- rf.timerState.tick():
			// 定时器到期
			rf.election()
		case rpcMsg := <- rf.rpcCh:
			switch rpcMsg.rpcType {
			case AppendEntryRpc:
			case RequestVoteRpc:
			case InstallSnapshotRpc:
			}
		}
	}
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

	rf.waitRpcResult(finishCh)
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election() {

	// 成为候选者
	if !rf.becomeCandidate() {
		return
	}

	// 发送 RV 请求
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	args := RequestVote{
		term:        rf.hardState.currentTerm(),
		candidateId: rf.peerState.myId(),
	}
	for id, addr := range rf.peerState.peersMap() {
		if rf.peerState.isMe(id) {
			continue
		}

		go func(id NodeId, addr NodeAddr) {

			var msg finishMsg
			rf.leaderState.setRpcBusy(id, true)
			defer func() {
				rf.leaderState.setRpcBusy(id, false)
				if _, ok := <-ctx.Done(); !ok {
					finishCh <- msg
				}
			}()

			res := &RequestVoteReply{}
			rpcErr := rf.transport.RequestVote(addr, args, res)

			if rpcErr != nil {
				log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, rpcErr))
				msg = finishMsg{msgType: RpcFailed}
				return
			}

			if res.voteGranted {
				// 成功获得选票
				msg = finishMsg{msgType: Success}
				return
			}

			if res.term > rf.hardState.currentTerm() {
				// 当前任期数落后，降级为 Follower
				msg = finishMsg{msgType: Degrade, term: res.term}
			}
		}(id, addr)
	}

	if rf.waitRpcResult(finishCh) {
		rf.becomeLeader()
	}
}

// msgCh 日志复制协程 -> 主协程，通知协程的任务完成
func (rf *raft) waitRpcResult(finishCh chan finishMsg) bool {
	count := 1
	successCnt := 1
	for msg := range finishCh {
		if msg.msgType == Degrade && rf.becomeFollower(Candidate, msg.term) {
			break
		}
		if msg.msgType == Success {
			successCnt += 1
		}
		if successCnt >= rf.peerState.majority() {
			return true
		}
		count += 1
		if count >= rf.peerState.peersCnt() {
			return false
		}
	}

	return false
}

// Follower 和 Candidate 接收到来自 Leader 的 AppendEntries 调用
func (rf *raft) handleCommand(args AppendEntry, res *AppendEntryReply) error {

	// 重置选举计时器
	rf.timerState.setElectionTimer()

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

	// 任期数落后或相等，如果是候选者，需要降级
	if rf.roleState.getRoleStage() == Candidate && !rf.becomeFollower(Candidate, args.term) {
		return nil
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
		stage := rf.roleState.getRoleStage()
		if stage != Follower && !rf.becomeFollower(stage, argsTerm) {
			return fmt.Errorf("角色降级失失败")
		}
		setTermErr := rf.hardState.setTerm(argsTerm)
		if setTermErr != nil {
			return fmt.Errorf("设置 term 值失败：%w", setTermErr)
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
			voteErr := rf.hardState.vote(args.candidateId)
			if voteErr != nil {
				log.Println(fmt.Errorf("投票失败：%w", voteErr))
			} else {
				res.voteGranted = true
			}
		}
	}

	if res.voteGranted {
		rf.timerState.setElectionTimer()
	}

	return nil
}

// Follower 接收来自 Leader 的 InstallSnapshot 调用
func (rf *raft) handleSnapshot(args InstallSnapshot, res *InstallSnapshotReply) error {

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

// 处理客户端请求
func (rf *raft) handleClientCmd(args ClientApply, res *ClientApplyReply) error {

	// 重置心跳计时器
	rf.timerState.setHeartbeatTimer()

	if !rf.isLeader() {
		res.status = NotLeader
		res.leader = rf.peerState.getLeader()
		return nil
	}

	res.status = OK

	// Leader 先将日志添加到内存
	addEntryErr := rf.addEntry(Entry{Term: rf.hardState.currentTerm(), Data: args.data})
	if addEntryErr != nil {
		return fmt.Errorf("leader 添加客户端日志失败：%w", addEntryErr)
	}

	// 给各节点发送日志条目
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	for id := range rf.peerState.peersMap() {
		// 不用给自己发，正在复制日志的不发
		if rf.peerState.isMe(id) || rf.leaderState.isRpcBusy(id) {
			continue
		}
		// 发送日志
		go rf.replicationTo(ctx, id, finishCh, true)
	}

	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	if !rf.waitRpcResult(finishCh) {
		cancel()
		return fmt.Errorf("rpc 完成，但日志未复制到多数节点")
	}
	cancel()

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目并应用到状态季节
	updateCmtErr := rf.updateLeaderCommit()
	if updateCmtErr != nil {
		return fmt.Errorf("leader 更新 commitIndex 失败：%w", updateCmtErr)
	}

	// 当日志量超过阈值时，生成快照
	if !rf.snapshotState.needGenSnapshot(rf.softState.softCommitIndex()) {
		return nil
	}
	bytes, serializeErr := rf.fsm.Serialize()
	if serializeErr != nil {
		return fmt.Errorf("从状态机序列化快照失败：%w", serializeErr)
	}

	// 快照数据发送给所有 Follower 节点
	rf.sendSnapshot(bytes)
	return nil
}

func (rf *raft) sendSnapshot(bytes []byte) {
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for id, addr := range rf.peerState.peersMap() {
		go rf.snapshotTo(ctx, id, addr, bytes, finishCh)
	}
	rf.waitRpcResult(finishCh)
}

// Leader 给某个节点发送心跳/日志
func (rf *raft) replicationTo(ctx context.Context, id NodeId, msgCh chan finishMsg, withEntry bool) {
	var msg finishMsg
	rf.leaderState.setRpcBusy(id, true)
	defer func() {
		rf.leaderState.setRpcBusy(id, false)

		if _, ok := <-ctx.Done(); !ok {
			msgCh <- msg
		}
	}()

	// 发起 RPC 调用
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

	// 处理 RPC 调用结果
	if err != nil {
		log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
		msg = finishMsg{msgType: RpcFailed}
		return
	}

	if res.success {
		msg = finishMsg{msgType: Success}
		return
	}

	if res.term > rf.hardState.currentTerm() {
		// 当前任期数落后，降级为 Follower
		msg = finishMsg{msgType: Degrade, term: res.term}
	} else {
		// Follower 和 Leader 的日志不匹配，进行日志追赶
		go rf.appendPeerEntry(id, addr)
	}
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
		if res.term > rf.hardState.currentTerm() && !rf.becomeFollower(Leader, res.term) {
			// 如果任期数小，降级为 Follower
			return
		}
		if res.success {
			return
		}

		// 确保下面对操作在 Leader 角色下完成
		lock := rf.roleState.lock(Leader)
		if !lock {
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

		rf.roleState.unlock()
	}
}

func (rf *raft) completeEntries(id NodeId, addr NodeAddr) {

	rl := rf.leaderState
	for {
		if rl.peerNextIndex(id)-1 == rf.lastLogIndex() {
			return
		}
		// 缺失的日志太多时，直接发送快照
		snapshot := rf.snapshotState.getSnapshot()
		finishCh := make(chan finishMsg)
		if rl.peerNextIndex(id) <= snapshot.LastIndex {
			rf.snapshotTo(context.Background(), id, addr, snapshot.Data, finishCh)
			msg := <-finishCh
			if msg.msgType != Success {
				if msg.msgType == Degrade && rf.becomeFollower(Leader, msg.term) {
					return
				}
			}

			if rf.roleState.lock(Leader) {
				rf.leaderState.setMatchAndNextIndex(id, snapshot.LastIndex, snapshot.LastIndex+1)
				rf.roleState.unlock()
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
		rpcErr := rf.transport.AppendEntries(addr, args, res)

		if rpcErr != nil {
			log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, rpcErr))
			return
		}
		if res.term > rf.hardState.currentTerm() && rf.becomeFollower(Leader, res.term) {
			// 如果任期数小，降级为 Follower
			return
		}

		// 向后补充
		if rf.roleState.lock(Leader) {
			rf.leaderState.setMatchAndNextIndex(id, rl.peerNextIndex(id), rl.peerNextIndex(id)+1)
			rf.roleState.unlock()
		}
	}
}

func (rf *raft) snapshotTo(ctx context.Context, id NodeId, addr NodeAddr, data []byte, finishCh chan finishMsg) {
	var msg finishMsg
	defer func() {
		if _, ok := <-ctx.Done(); !ok {
			finishCh <- msg
		}
	}()
	if rf.peerState.isMe(id) {
		// 自己保存快照
		newSnapshot := Snapshot{
			LastIndex: rf.softState.softCommitIndex(),
			LastTerm:  rf.hardState.currentTerm(),
			Data:      data,
		}
		if saveErr := rf.snapshotState.save(newSnapshot); saveErr != nil {
			log.Println(fmt.Errorf("本地保存快照失败：%w", saveErr))
		}
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
		log.Println(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
		msg = finishMsg{msgType: RpcFailed}
		return
	}
	if res.term > rf.hardState.currentTerm() {
		// 如果任期数小，降级为 Follower
		msg = finishMsg{msgType: Degrade, term: res.term}
		return
	}
	msg = finishMsg{msgType: Success}
}

func (rf *raft) handleServerAdd(args AddServer, res *AddServerReply) error {

	if !rf.isLeader() {
		res.status = NotLeader
		res.leader = rf.peerState.getLeader()
		return nil
	}

	return nil
}

func (rf *raft) handleServerRemove(args RemoveServer, res *RemoveServerReply) error {

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

func (rf *raft) becomeLeader() bool {
	// 重置心跳计时器
	rf.timerState.setHeartbeatTimer()

	// 给各个节点发送心跳，建立权柄
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for id := range rf.peerState.peersMap() {
		if rf.peerState.isMe(id) {
			continue
		}

		go rf.replicationTo(ctx, id, finishCh, false)
	}

	// 权柄建立成功，将自己置为 Leader
	if rf.waitRpcResult(finishCh) {
		rf.setRoleStage(Leader)
		rf.roleState.lock(Leader)
		defer rf.roleState.unlock()

		rf.peerState.setLeader(rf.peerState.myId())
		for id := range rf.peerState.peersMap() {
			// 初始化保存的各节点状态
			rf.leaderState.setMatchAndNextIndex(id, 0, rf.lastLogIndex()+1)
			rf.leaderState.initNotifier(id)
		}
		return true
	}
	return false
}

func (rf *raft) becomeCandidate() bool {
	// 重置选举计时器
	rf.timerState.setElectionTimer()

	// 增加 term 数
	err := rf.hardState.termAddAndVote(1, rf.peerState.myId())
	if err != nil {
		log.Println(err)
		return false
	}
	// 角色置为候选者
	rf.setRoleStage(Candidate)
	return true
}

// 降级为 Follower
func (rf *raft) becomeFollower(stage RoleStage, term int) bool {
	if !rf.roleState.lock(stage) {
		return false
	}
	defer rf.roleState.unlock()

	err := rf.hardState.setTerm(term)
	if err != nil {
		log.Println(fmt.Errorf("降级失败%w", err))
		return false
	}
	rf.setRoleStage(Follower)
	return true
}

func (rf *raft) setRoleStage(stage RoleStage) {
	rf.roleState.setRoleStage(stage)
	if stage == Leader {
		rf.peerState.setLeader(rf.peerState.myId())
		rf.timerState.setHeartbeatTimer()
	} else {
		rf.timerState.setElectionTimer()
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
