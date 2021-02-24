package core

import (
	"fmt"
	"log"
)

type raft struct {
	roleState   *RoleState        // 当前节点的角色
	persister   SnapshotPersister // 快照生成器
	fsm         Fsm               // 客户端状态机
	transport   Transport         // 发送请求的接口
	hardState   *HardState        // 需要持久化存储的状态
	softState   *SoftState        // 保存在内存中的实时状态
	peerState   *PeerState        // 对等节点状态和路由表
	leaderState *LeaderState      // 节点是 Leader 时，保存在内存中的状态
	timerState  *timerState       // 计时器状态
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
		transport:   config.Transport,
		hardState:   &hardState,
		softState:   NewSoftState(),
		peerState:   NewPeerState(config.Peers, config.Me),
		leaderState: NewLeaderState(),
		timerState:  NewTimerState(config),
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

// 更新 Leader 的提交索引 todo 单元测试
func (rf *raft) updateLeaderCommitIndex() error {

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
	if !res.success || rf.nextIndex(id) != rf.lastLogIndex()+1 {
		err := rf.sendLogEntry(id, addr)
		if err != nil {
			log.Println(err)
		}
	}
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
	var voteCh,degradeCh,noopCh chan struct{}
	defer func() {
		close(voteCh)
		close(degradeCh)
		close(noopCh)
	}()

	for id, addr := range rf.peers() {
		if rf.isMe(id) {
			continue
		}
		go func(id NodeId, addr NodeAddr) {
			args := RequestVote{
				term:        rf.term(),
				candidateId: id,
			}
			res := &RequestVoteReply{}
			err = rf.transport.RequestVote(addr, args, res)
			if err != nil {
				log.Printf("调用rpc服务失败：%s%s\n", addr, err)
				return
			}
			if res.term <= rf.term() && res.voteGranted {
				// 成功获得选票
				voteCh <- struct{}{}
			} else if res.term > rf.term() {
				// 当前节点任期落后，则退出竞选
				err = rf.degrade(res.term)
				if err != nil {
					log.Println(err)
				}
				degradeCh <- struct{}{}
			} else {
				// 其它情况
				noopCh <- struct {}{}
			}
		}(id, addr)
	}

	// todo 使用 context，在函数退出时尽快结束协程
	voteCnt := 1
	count := 1
	for {
		select {
		case <-voteCh:
			voteCnt += 1
			count += 1
			if voteCnt >= rf.majority() || count >= len(rf.peers()) {
				rf.setRoleStage(Leader)
				return
			}
		case <-degradeCh:
			return
		case <-noopCh:
			count += 1
			if count >= len(rf.peers()) {
				return
			}
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
		err := rf.appendEntry(args.entries[0])
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
		var err error
		if rf.roleStage() != Follower {
			err = rf.degrade(argsTerm)
		} else {
			err = rf.setTerm(argsTerm)
		}
		if err != nil {
			log.Println(err)
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
		err := rf.transport.AppendEntries(addr, args, res)
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
				rf.setMatchAndNextIndex(id, snapshot.LastIndex, snapshot.LastIndex+1)
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
		err := rf.transport.AppendEntries(addr, args, res)
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
			LastTerm:  rf.term(),
			Data:      data,
		}
		return rf.persister.SaveSnapshot(newSnapshot)
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
