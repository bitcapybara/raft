package core

import (
	"context"
	"fmt"
	"log"
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
	rf.initTimer()
	go func() {
		tm := rf.timerState
		for {
			<-tm.timer.C
			if rf.isHeartbeatTick() {
				rf.heartbeat()
				rf.setHeartbeatTimer()
			} else if rf.isElectionTick() {
				rf.election()
				rf.setElectionTimer()
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
	for id := range rf.peers() {
		if rf.isMe(id) {
			continue
		}
		if rf.needHeartbeat(id) {
			rf.heartbeatTo(id)
		}
	}
	rf.setHeartbeatTimer()
}

// Leader 给某个节点发送心跳
func (rf *raft) heartbeatTo(id NodeId) {
	addr := rf.peers()[id]
	commitIndex := rf.commitIndex()
	if rf.isMe(id) {
		return
	}

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
	err := rf.transport.AppendEntries(addr, args, res)
	if err != nil {
		log.Print(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err))
		return
	}

	if res.term > term {
		// 当前任期数落后，降级为 Follower
		err = rf.degrade(res.term)
		if err != nil {
			log.Println(err)
		}
	}

	// Follower 和 Leader 的日志不匹配，则进行日志同步
	ctx := context.Background()
	msgCh := make(chan clientReqMsg)
	defer close(msgCh)
	if !res.success || rf.nextIndex(id) != rf.lastLogIndex()+1 {
		// Follower 节点忙于 AE 请求，不需要发送心跳
		rf.setAeState(id, true)
		// Follower 节点设置为 AE 空闲，可以发送心跳
		defer rf.setAeState(id, false)
		// 同步日志
		rf.findCorrectNextIndex(ctx, id, addr, msgCh)
		rf.completeEntries(ctx, id, addr, msgCh)
	}

	for msg := range msgCh {
		if msg.err != nil {
			log.Print(fmt.Errorf("日志同步失败：%s%w\n", addr, err))
		}
		break
	}
}

type electionMsg struct {
	vote    bool
	degrade bool
	noop    bool
	err     error
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election() {
	// 增加 term 数
	err := rf.setTerm(rf.term() + 1)
	if err != nil {
		log.Println(err)
	}
	// 角色置为候选者
	rf.setRoleStage(Candidate)
	// 投票给自己
	err = rf.vote(rf.me())
	if err != nil {
		log.Println(err)
	}
	// 发送 RV 请求
	ctx, cancel := context.WithCancel(context.Background())
	msgCh := make(chan electionMsg, len(rf.peers()))
	defer close(msgCh)
	for id, addr := range rf.peers() {
		if rf.isMe(id) {
			continue
		}
		go rf.sendRequestVote(ctx, msgCh, id, addr)
	}

	voteCnt := 1
	count := 1
	for msg := range msgCh {
		if msg.err != nil {
			log.Println(fmt.Errorf("RequestVote Rpc 调用失败：%w", err))
			continue
		}
		if msg.vote {
			voteCnt += 1
			count += 1
			if voteCnt >= rf.majority() {
				rf.setRoleStage(Leader)
			}
		} else if msg.noop {
			count += 1
		}
		if msg.degrade || count >= len(rf.peers()) {
			break
		}
	}
	cancel()
}

type rpcState struct {
	err error
}

func (rf *raft) sendRequestVote(ctx context.Context, msgCh chan electionMsg, id NodeId, addr NodeAddr) {
	args := RequestVote{
		term:        rf.term(),
		candidateId: id,
	}
	res := &RequestVoteReply{}
	finishCh := make(chan rpcState)
	go func() {
		err := rf.transport.RequestVote(addr, args, res)
		if err != nil {
			finishCh <- rpcState{err: fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err)}
		} else {
			finishCh <- rpcState{}
		}
	}()

	var err error
	select {
	case <-ctx.Done():
	case st := <-finishCh:
		if st.err != nil {
			msgCh <- electionMsg{err: err}
			break
		}
		if res.term <= rf.term() && res.voteGranted {
			// 成功获得选票
			msgCh <- electionMsg{vote: true}
		} else if res.term > rf.term() {
			// 当前节点任期落后，则退出竞选
			err = rf.degrade(res.term)
			if err != nil {
				msgCh <- electionMsg{err: err}
			}
			msgCh <- electionMsg{degrade: true}
		} else {
			// 其它情况
			msgCh <- electionMsg{noop: true}
		}
	}
}

// Follower 和 Candidate 接收到来自 Leader 的 AppendEntries 调用
func (rf *raft) handleCommand(args AppendEntry, res *AppendEntryReply) error {
	// 重置选举计时器
	rf.resetElectionTimer()

	// 判断 term
	rfTerm := rf.term()
	if args.term < rfTerm {
		// 发送请求的 Leader 任期数落后
		res.term = rfTerm
		res.success = false
		return nil
	}

	// 处理请求
	prevIndex := args.prevLogIndex
	if prevIndex > rf.lastLogIndex() || rf.logEntryTerm(prevIndex) != args.prevLogTerm {
		res.term = rfTerm
		res.success = false
		return nil
	}

	// 任期数落后或相等
	if rf.roleStage() == Candidate {
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
		if rf.lastLogIndex() >= newEntryIndex && rf.logEntryTerm(newEntryIndex) != args.term {
			rf.truncateEntries(prevIndex + 1)
		}

		// 将新条目添加到日志中
		err := rf.addEntry(args.entries[0])
		if err != nil {
			log.Println(err)
		}
	} else {
		// ========== 接收心跳 ==========
		rf.setLeader(args.leaderId)
		res.term = rf.term()
		res.success = true
	}

	// 更新提交索引
	leaderCommit := args.leaderCommit
	if leaderCommit > rf.commitIndex() {
		var err error
		if leaderCommit >= newEntryIndex {
			err = rf.setCommitIndex(newEntryIndex)
		} else {
			err = rf.setCommitIndex(leaderCommit)
		}
		return err
	}

	return nil
}

// Follower 和 Candidate 接收到来自 Candidate 的 RequestVote 调用
func (rf *raft) handleVoteReq(args RequestVote, res *RequestVoteReply) error {
	var err error
	argsTerm := args.term

	rfTerm := rf.term()
	if argsTerm < rfTerm {
		// 拉票的候选者任期落后，不投票
		res.term = rfTerm
		res.voteGranted = false
		return nil
	}

	if argsTerm > rfTerm {
		// 角色降级
		if rf.roleStage() != Follower {
			err = rf.degrade(argsTerm)
		} else {
			err = rf.setTerm(argsTerm)
		}
		if err != nil {
			err = fmt.Errorf("角色降级失败：%w", err)
		}
	}

	res.term = argsTerm
	res.voteGranted = false
	votedFor := rf.votedFor()
	if votedFor == "" || votedFor == args.candidateId {
		// 当前节点是追随者且没有投过票
		lastIndex := rf.lastLogIndex()
		lastTerm := rf.logEntryTerm(lastIndex)
		// 候选者的日志比当前节点的日志要新，则投票
		if args.lastLogTerm > lastTerm || (args.lastLogTerm == lastTerm && args.lastLogIndex >= lastIndex) {
			err = rf.vote(args.candidateId)
			if err != nil {
				err = fmt.Errorf("")
			}
			res.voteGranted = true
		}
	}

	if res.voteGranted {
		rf.resetElectionTimer()
	}

	return err
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
	err := rf.saveSnapshot(snapshot)
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

type clientReqMsg struct {
	degrade bool
	success bool
	err     error
}

// 给各节点发送客户端日志
func (rf *raft) handleClientReq(args ClientRequest, res *ClientResponse) error {
	if !rf.isLeader() {
		res.ok = false
		res.leaderId = rf.leaderId()
		return nil
	}

	// Leader 先将日志添加到内存
	err := rf.addEntry(Entry{Term: rf.term(), Data: args.data})
	if err != nil {
		log.Println(err)
	}

	// 给各节点发送日志条目
	ctx, cancel := context.WithCancel(context.Background())
	msgCh := make(chan clientReqMsg)
	defer close(msgCh)
	for id, addr := range rf.peers() {
		// 不用给自己发
		if rf.isMe(id) {
			continue
		}
		go func(id NodeId, addr NodeAddr) {
			// Follower 节点忙于 AE 请求，不需要发送心跳
			rf.setAeState(id, true)

			// Follower 节点设置为 AE 空闲，可以发送心跳
			defer rf.setAeState(id, false)

			rf.findCorrectNextIndex(ctx, id, addr, msgCh)
			rf.completeEntries(ctx, id, addr, msgCh)
		}(id, addr)
	}

	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	successCnt := 0
	count := 1
	for msg := range msgCh {
		if msg.degrade {
			cancel()
			return fmt.Errorf("term 过期，降级为 Follower")
		} else if msg.success {
			successCnt += 1
		} else if msg.err != nil {
			log.Println(err)
		}
		count += 1
		if count >= len(rf.peers()) {
			break
		}
	}
	cancel()

	if successCnt < rf.majority() {
		return fmt.Errorf("rpc 完成，但日志未复制到多数节点")
	}

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目并应用到状态季节
	err = rf.updateLeaderCommit()
	if err != nil {
		log.Println(err)
	}

	// 当日志量超过阈值时，生成快照
	if !rf.needGenSnapshot(rf.commitIndex()) {
		return nil
	}
	bytes, err := rf.fsm.Serialize()
	if err != nil {
		return nil
	}

	// 快照数据发送给所有 Follower 节点
	for id, addr := range rf.peers() {
		go func(id NodeId, addr NodeAddr) {
			err = rf.sendSnapshot(id, addr, bytes)
			if err != nil {
				log.Println(err)
			}
		}(id, addr)
	}
	return nil
}

func (rf *raft) findCorrectNextIndex(ctx context.Context, id NodeId, addr NodeAddr, msgCh chan clientReqMsg) {
	// Follower 节点中必须存在第 prevIndex 条日志，才能接受第 nextIndex 条日志
	// 如果 Follower 节点缺少很多日志，需要找到缺少的第一条日志的索引
	for rf.nextIndex(id) >= 0 {
		select {
		case <-ctx.Done():
			return
		default:
			prevIndex := rf.nextIndex(id) - 1
			args := AppendEntry{
				term:         rf.term(),
				leaderId:     rf.me(),
				prevLogIndex: prevIndex,
				prevLogTerm:  rf.logEntryTerm(prevIndex),
				leaderCommit: rf.commitIndex(),
				entries:      rf.entries(prevIndex, rf.nextIndex(id)),
			}
			res := &AppendEntryReply{}
			err := rf.transport.AppendEntries(addr, args, res)
			if err != nil {
				msgCh <- clientReqMsg{err: fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err)}
				return
			}
			if res.term > rf.term() {
				// 如果任期数小，降级为 Follower
				err = rf.degrade(res.term)
				if err != nil {
					log.Println(err)
				}
				msgCh <- clientReqMsg{degrade: true}
				return
			}
			if res.success {
				return
			}

			// 向前继续查找 Follower 缺少的第一条日志的索引
			rf.setNextIndex(id, rf.nextIndex(id)-1)
		}
	}
}

// 给 Follower 发送缺失的日志，发送的日志的索引一直递增到最新
func (rf *raft) completeEntries(ctx context.Context, id NodeId, addr NodeAddr, msgCh chan clientReqMsg) {
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if rf.nextIndex(id) - 1 == rf.lastLogIndex() {
				msgCh <- clientReqMsg{success: true}
				return
			}
			// 缺失的日志太多时，直接发送快照
			snapshot := rf.snapshot()
			if rf.nextIndex(id) <= snapshot.LastIndex {
				err = rf.sendSnapshot(id, addr, snapshot.Data)
				if err != nil {
					log.Println(err)
				} else {
					rf.setMatchAndNextIndex(id, snapshot.LastIndex, snapshot.LastIndex+1)
				}
			}

			prevIndex := rf.nextIndex(id) - 1
			args := AppendEntry{
				term:         rf.term(),
				leaderId:     rf.me(),
				prevLogIndex: prevIndex,
				prevLogTerm:  rf.logEntryTerm(prevIndex),
				leaderCommit: rf.commitIndex(),
				entries:      rf.entries(prevIndex, rf.nextIndex(id)),
			}
			res := &AppendEntryReply{}
			err = rf.transport.AppendEntries(addr, args, res)
			if err != nil {
				msgCh <- clientReqMsg{err: fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err)}
				return
			}
			if res.term > rf.term() {
				// 如果任期数小，降级为 Follower
				err = rf.degrade(res.term)
				if err != nil {
					log.Println(err)
				}
				msgCh <- clientReqMsg{degrade: true}
			}

			// 向后补充
			rf.setMatchAndNextIndex(id, rf.nextIndex(id), rf.nextIndex(id)+1)
		}
	}
}

func (rf *raft) sendSnapshot(id NodeId, addr NodeAddr, data []byte) error {
	if rf.isMe(id) {
		// 自己保存快照
		newSnapshot := Snapshot{
			LastIndex: rf.commitIndex(),
			LastTerm:  rf.term(),
			Data:      data,
		}
		return rf.saveSnapshot(newSnapshot)
	}
	args := InstallSnapshot{
		term:              rf.term(),
		leaderId:          rf.me(),
		lastIncludedIndex: rf.commitIndex(),
		lastIncludedTerm:  rf.logEntryTerm(rf.commitIndex()),
		offset:            0,
		data:              data,
		done:              true,
	}
	res := &InstallSnapshotReply{}
	err := rf.transport.InstallSnapshot(addr, args, res)
	if err != nil {
		return fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err)
	}
	if res.term > rf.term() {
		// 如果任期数小，降级为 Follower
		return rf.degrade(res.term)
	}

	return nil
}

// 当前节点是不是 Leader
func (rf *raft) isLeader() bool {
	roleStage := rf.roleState.getRoleStage()
	leaderIsMe := rf.peerState.leaderIsMe()
	return roleStage == Leader && leaderIsMe
}

// 降级为 Follower
func (rf *raft) degrade(term int) error {
	if rf.roleStage() == Follower {
		return nil
	}
	// 更新状态
	rf.setRoleStage(Follower)
	return rf.setTerm(term)
}

// 把日志应用到状态机
func (rf *raft) applyFsm() error {
	commitIndex := rf.commitIndex()
	lastApplied := rf.lastApplied()

	for commitIndex > lastApplied {
		entry := rf.logEntry(lastApplied + 1)
		err := rf.fsm.Apply(entry.Data)
		if err != nil {
			return fmt.Errorf("应用状态机失败：%w", err)
		}
		lastApplied = rf.lastAppliedAdd()
	}

	return nil
}

// 更新 Leader 的提交索引
func (rf *raft) updateLeaderCommit() error {

	indexCnt := make(map[int]int)
	peers := rf.peers()
	//
	for id := range peers {
		indexCnt[rf.matchIndex(id)] = 1
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
		if cnt >= rf.majority() && index > maxMajorityMatch {
			maxMajorityMatch = index
		}
	}

	if rf.commitIndex() < maxMajorityMatch {
		return rf.setCommitIndex(maxMajorityMatch)
	}

	return nil
}

// ==================== RoleState ====================

func (rf *raft) roleStage() RoleStage {
	return rf.roleState.getRoleStage()
}

func (rf *raft) setRoleStage(stage RoleStage) {
	rf.roleState.setRoleStage(stage)
	if stage == Leader {
		rf.setLeader(rf.me())
		rf.resetHeartbeatTimer()
	} else {
		rf.resetElectionTimer()
	}
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

// 删掉指定索引及以后的所有条目
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

func (rf *raft) addEntry(entry Entry) error {
	return rf.hardState.appendEntry(entry)
}

// ==================== SoftState ====================

func (rf *raft) commitIndex() int {
	return rf.softState.softCommitIndex()
}

func (rf *raft) setLastApplied(index int) {
	rf.softState.setLastApplied(index)
}

func (rf *raft) lastAppliedAdd() int {
	return rf.softState.lastAppliedAdd()
}

func (rf *raft) lastApplied() int {
	return rf.softState.softLastApplied()
}

func (rf *raft) setCommitIndex(index int) error {
	rf.softState.setCommitIndex(index)
	return rf.applyFsm()
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

// ==================== timerState ====================

func (rf *raft) initTimer() {
	rf.timerState.initTimerState(rf.peers())
}

func (rf *raft) needHeartbeat(id NodeId) bool {
	return !rf.timerState.isAeBusy(id)
}

func (rf *raft) setHeartbeatTimer() {
	rf.timerState.setHeartbeatTimer()
}

func (rf *raft) setElectionTimer() {
	rf.timerState.setElectionTimer()
}

func (rf *raft) setAeState(id NodeId, isBusy bool) {
	rf.timerState.setAeState(id, isBusy)
}

func (rf *raft) resetElectionTimer() {
	rf.timerState.resetElectionTimer()
}

func (rf *raft) resetHeartbeatTimer() {
	rf.timerState.resetHeartbeatTimer()
}

// ==================== snapshotState ====================

func (rf *raft) saveSnapshot(snapshot Snapshot) error {
	return rf.snapshotState.save(snapshot)
}

func (rf *raft) snapshot() *Snapshot {
	return rf.snapshotState.getSnapshot()
}

func (rf *raft) needGenSnapshot(commitIndex int) bool {
	return rf.snapshotState.needGenSnapshot(commitIndex)
}
