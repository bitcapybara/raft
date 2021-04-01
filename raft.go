package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

type finishMsgType uint8

const (
	Success finishMsgType = iota
	RpcFailed
	Degrade
	Timeout
)

type finishMsg struct {
	msgType finishMsgType
	term    int
}

// 配置参数
type Config struct {
	Fsm                Fsm
	RaftStatePersister RaftStatePersister
	SnapshotPersister  SnapshotPersister
	Transport          Transport
	Logger             Logger
	Peers              map[NodeId]NodeAddr
	Me                 NodeId
	Role               RoleStage
	ElectionMinTimeout int
	ElectionMaxTimeout int
	HeartbeatTimeout   int
	MaxLogLength       int
}

// 客户端状态机接口
type Fsm interface {
	// 参数实际上是 Entry 的 Data 字段
	// 返回值是应用状态机后的结果
	Apply([]byte) error

	// 生成快照二进制数据
	Serialize() ([]byte, error)
}

type raft struct {
	fsm           Fsm            // 客户端状态机
	transport     Transport      // 发送请求的接口
	logger        Logger         // 日志打印
	roleState     *RoleState     // 当前节点的角色
	hardState     *HardState     // 需要持久化存储的状态
	softState     *SoftState     // 保存在内存中的实时状态
	peerState     *PeerState     // 对等节点状态和路由表
	leaderState   *LeaderState   // 节点是 Leader 时，保存在内存中的状态
	timerState    *timerState    // 计时器状态
	snapshotState *snapshotState // 快照状态

	rpcCh  chan rpc      // 主线程接收 rpc 消息
	exitCh chan struct{} // 当前节点离开节点，退出程序
}

func newRaft(config Config) *raft {
	if config.ElectionMinTimeout > config.ElectionMaxTimeout {
		panic("ElectionMinTimeout 不能大于 ElectionMaxTimeout！")
	}
	raftPst := config.RaftStatePersister

	var raftState RaftState
	if raftPst != nil {
		rfState, err := raftPst.LoadRaftState()
		if err != nil {
			panic(fmt.Sprintf("持久化器加载 RaftState 失败：%s\n", err))
		} else {
			raftState = rfState
		}
	} else {
		panic("缺失 RaftStatePersister!")
	}
	hardState := raftState.toHardState(raftPst)

	return &raft{
		fsm:           config.Fsm,
		transport:     config.Transport,
		logger:        config.Logger,
		roleState:     newRoleState(config.Role),
		hardState:     &hardState,
		softState:     newSoftState(),
		peerState:     newPeerState(config.Peers, config.Me),
		leaderState:   newLeaderState(),
		timerState:    newTimerState(config),
		snapshotState: newSnapshotState(config),
		rpcCh:         make(chan rpc),
		exitCh:        make(chan struct{}),
	}
}

func (rf *raft) raftRun(rpcCh chan rpc) {
	rf.rpcCh = rpcCh
	go func() {
		for {
			select {
			case <-rf.exitCh:
				return
			default:
			}
			switch rf.roleState.getRoleStage() {
			case Leader:
				rf.logger.Trace("开启runLeader()循环")
				rf.runLeader()
			case Candidate:
				rf.logger.Trace("开启runCandidate()循环")
				rf.runCandidate()
			case Follower:
				rf.logger.Trace("开启runFollower()循环")
				rf.runFollower()
			case Learner:
				rf.logger.Trace("开启runLearner()循环")
				rf.runLearner()
			}
		}
	}()
}

func (rf *raft) runLeader() {
	rf.logger.Trace("进入 runLeader()")
	// 初始化心跳定时器
	rf.timerState.setHeartbeatTimer()
	rf.logger.Trace("初始化心跳定时器成功")

	// 开启日志复制循环
	rf.runReplication()
	rf.logger.Trace("开启日志复制循环")

	// 节点退出 Leader 状态，收尾工作
	defer func() {
		for _, st := range rf.leaderState.followerState {
			close(st.stopCh)
		}
		rf.logger.Trace("退出 runLeader()，关闭各个 replication 的 stopCh")
	}()

	for rf.roleState.getRoleStage() == Leader {
		select {
		case msg := <-rf.rpcCh:
			if transfereeId, busy := rf.leaderState.isTransferBusy(); busy {
				// 如果正在进行领导权转移
				rf.logger.Trace("节点正在进行领导权转移，请求失败！")
				msg.res <- rpcReply{err: fmt.Errorf("正在进行领导权转移，请求失败！")}
				rf.checkTransfer(transfereeId)
			} else {
				switch msg.rpcType {
				case AppendEntryRpc:
					rf.logger.Trace("接收到 AppendEntryRpc 请求")
					rf.handleCommand(msg)
				case RequestVoteRpc:
					rf.logger.Trace("接收到 RequestVoteRpc 请求")
					rf.handleVoteReq(msg)
				case ApplyCommandRpc:
					rf.logger.Trace("接收到 ApplyCommandRpc 请求")
					rf.handleClientCmd(msg)
				case ChangeConfigRpc:
					rf.logger.Trace("接收到 ChangeConfigRpc 请求")
					rf.handleConfiguration(msg)
				case TransferLeadershipRpc:
					rf.logger.Trace("接收到 TransferLeadershipRpc 请求")
					rf.handleTransfer(msg)
				case AddNewNodeRpc:
					rf.logger.Trace("接收到 AddNewNodeRpc 请求")
					rf.handleNewNode(msg)
				}
			}
		case <-rf.timerState.tick():
			rf.logger.Trace("心跳计时器到期，开始发送心跳")
			stopCh := make(chan struct{})
			finishCh := rf.heartbeat(stopCh)
			successCnt := 1
			count := 1
			end := false
			for !end {
				select {
				case <-time.After(rf.timerState.heartbeatDuration()):
					rf.logger.Trace("操作超时退出")
					end = true
				case msg := <-finishCh:
					if msg.msgType == Degrade && rf.becomeFollower(msg.term) {
						rf.logger.Trace("降级为 Follower")
						end = true
						break
					}
					if msg.msgType == Success {
						rf.logger.Trace("成功获取到一个心跳结果")
						successCnt += 1
					}
					if successCnt >= rf.peerState.majority() {
						rf.logger.Trace("心跳已成功发送给多数节点")
						end = true
						break
					}
					count += 1
					if count >= rf.peerState.peersCnt() {
						rf.logger.Trace("已接收所有响应，成功节点数未达到多数")
						end = true
						break
					}
				}
			}
			close(finishCh)
			close(stopCh)
		case id := <-rf.leaderState.done:
			if transfereeId, busy := rf.leaderState.isTransferBusy(); busy && transfereeId == id {
				rf.logger.Trace("领导权转移的目标节点日志复制结束，开始领导权转移")
				rf.checkTransfer(transfereeId)
				rf.logger.Trace("领导权转移结束")
			}
		case msg := <-rf.leaderState.stepDownCh:
			// 接收到降级消息
			rf.logger.Trace("接收到降级消息")
			if rf.becomeFollower(msg) {
				rf.logger.Trace("Leader降级成功")
				return
			}
		}
	}
}

func (rf *raft) runCandidate() {
	// 初始化选举计时器
	rf.timerState.setElectionTimer()
	rf.logger.Trace("初始化选举计时器成功")
	// 开始选举
	stopCh := make(chan struct{})
	defer close(stopCh)
	rf.logger.Trace("开始选举")
	finishCh := rf.election(stopCh)

	successCnt := 1
	for rf.roleState.getRoleStage() == Candidate {
		select {
		case <-rf.timerState.tick():
			// 开启下一轮选举
			rf.logger.Trace("选举计时器到期，开始下一轮选举")
			return
		case msg := <-rf.rpcCh:
			switch msg.rpcType {
			case AppendEntryRpc:
				rf.logger.Trace("接收到 AppendEntryRpc 请求")
				rf.handleCommand(msg)
			case RequestVoteRpc:
				rf.logger.Trace("接收到 RequestVoteRpc 请求")
				rf.handleVoteReq(msg)
			}
		case msg, ok := <-finishCh:
			if !ok {
				rf.logger.Trace("接收到 preVote 失败消息")
				break
			}
			// 降级
			if msg.msgType == Degrade && rf.becomeFollower(msg.term) {
				rf.logger.Trace("降级为 Follower")
				return
			}
			if msg.msgType == Success {
				rf.logger.Trace("成功获取到一个投票")
				successCnt += 1
			}
			// 升级
			if successCnt >= rf.peerState.majority() && rf.becomeLeader() {
				rf.logger.Trace("获取到多数节点投票，升级为 Leader")
				return
			}
		}
	}
}

func (rf *raft) runFollower() {
	// 初始化选举计时器
	rf.timerState.setElectionTimer()
	rf.logger.Trace("初始化选举计时器成功")
	for rf.roleState.getRoleStage() == Follower {
		select {
		case <-rf.timerState.tick():
			// 成为候选者
			rf.logger.Trace("选举计时器到期，开启新一轮选举")
			rf.becomeCandidate()
			return
		case msg := <-rf.rpcCh:
			switch msg.rpcType {
			case AppendEntryRpc:
				rf.logger.Trace("接收到 AppendEntryRpc 请求")
				rf.handleCommand(msg)
			case RequestVoteRpc:
				rf.logger.Trace("接收到 RequestVoteRpc 请求")
				rf.handleVoteReq(msg)
			case InstallSnapshotRpc:
				rf.logger.Trace("接收到 InstallSnapshotRpc 请求")
				rf.handleSnapshot(msg)
			}
		}
	}
}

func (rf *raft) runLearner() {
	for rf.roleState.getRoleStage() == Learner {
		select {
		case msg := <-rf.rpcCh:
			switch msg.rpcType {
			case AppendEntryRpc:
				rf.logger.Trace("接收到 AppendEntryRpc 请求")
				rf.handleCommand(msg)
			}
		}
	}
}

// ==================== logic process ====================

func (rf *raft) heartbeat(stopCh chan struct{}) chan finishMsg {

	// 重置心跳计时器
	rf.timerState.setHeartbeatTimer()
	rf.logger.Trace("重置心跳计时器成功")

	finishCh := make(chan finishMsg)

	for id := range rf.peerState.peers() {
		if rf.peerState.isMe(id) || rf.leaderState.isRpcBusy(id) {
			rf.logger.Trace(fmt.Sprintf("自身和忙节点，不发送心跳。id=%s", id))
			continue
		}
		rf.logger.Trace(fmt.Sprintf("给 id=%s 的节点发送心跳", id))
		go rf.replicationTo(id, finishCh, stopCh, EntryHeartbeat)
	}

	return finishCh
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election(stopCh chan struct{}) <-chan finishMsg {
	// pre-vote
	preVoteFinishCh := rf.sendRequestVote(stopCh)
	defer close(preVoteFinishCh)

	if !rf.waitRpcResult(preVoteFinishCh) {
		rf.logger.Trace("preVote 失败，退出选举")
		return preVoteFinishCh
	}

	// 增加 term 数
	err := rf.hardState.termAddAndVote(1, rf.peerState.myId())
	if err != nil {
		rf.logger.Error(fmt.Errorf("增加term，设置votedFor失败%w", err).Error())
	}
	rf.logger.Trace(fmt.Sprintf("增加 term 数，开始发送 RequestVote 请求。term=%d", rf.hardState.currentTerm()))

	return rf.sendRequestVote(stopCh)
}

func (rf *raft) sendRequestVote(stopCh <-chan struct{}) chan finishMsg {
	// 发送 RV 请求
	finishCh := make(chan finishMsg)

	args := RequestVote{
		term:        rf.hardState.currentTerm(),
		candidateId: rf.peerState.myId(),
	}
	for id, addr := range rf.peerState.peers() {
		if rf.peerState.isMe(id) {
			continue
		}

		go func(id NodeId, addr NodeAddr) {

			var msg finishMsg
			defer func() {
				select {
				case <-stopCh:
					rf.logger.Trace("接收到 stopCh 消息")
				default:
					finishCh <- msg
				}
			}()

			res := &RequestVoteReply{}
			rf.logger.Trace(fmt.Sprintf("发送投票请求：%+v", args))
			rpcErr := rf.transport.RequestVote(addr, args, res)

			if rpcErr != nil {
				rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w", addr, rpcErr).Error())
				msg = finishMsg{msgType: RpcFailed}
				return
			}

			if res.voteGranted {
				// 成功获得选票
				rf.logger.Trace(fmt.Sprintf("成功获得来自 id=%s 的选票", id))
				msg = finishMsg{msgType: Success}
				return
			}

			term := rf.hardState.currentTerm()
			if res.term > term {
				// 当前任期数落后，降级为 Follower
				rf.logger.Trace(fmt.Sprintf("当前任期数落后，降级为 Follower, term=%d, resTerm=%d", term, res.term))
				msg = finishMsg{msgType: Degrade, term: res.term}
			}
		}(id, addr)
	}

	return finishCh
}

// msgCh 日志复制协程 -> 主协程，通知协程的任务完成
func (rf *raft) waitRpcResult(finishCh <-chan finishMsg) bool {
	count := 1
	successCnt := 1
	end := false
	for !end {
		select {
		case <-time.After(rf.timerState.heartbeatDuration()):
			rf.logger.Trace("操作超时退出")
			end = true
		case msg := <-finishCh:
			if msg.msgType == Degrade && rf.becomeFollower(msg.term) {
				rf.logger.Trace("接收到降级请求并降级成功")
				end = true
				break
			}
			if msg.msgType == Success {
				rf.logger.Trace("接收到成功响应")
				successCnt += 1
			}
			if successCnt >= rf.peerState.majority() {
				rf.logger.Trace("请求已成功发送给多数节点")
				return true
			}
			count += 1
			if count >= rf.peerState.peersCnt() {
				rf.logger.Trace("已接收所有响应，成功节点数未达到多数")
				return false
			}
		}
	}

	return false
}

func (rf *raft) runReplication() {
	for id, addr := range rf.peerState.peers() {
		rf.addReplication(id, addr)
	}
}

func (rf *raft) addReplication(id NodeId, addr NodeAddr) {
	st, ok := rf.leaderState.followerState[id]
	if !ok {
		rf.logger.Trace(fmt.Sprintf("生成节点 id=%s 的 Replication 对象", id))
		st = &Replication{
			id:         id,
			addr:       addr,
			nextIndex:  rf.lastLogIndex() + 1,
			matchIndex: 0,
			stepDownCh: rf.leaderState.stepDownCh,
			stopCh:     make(chan struct{}),
			triggerCh:  make(chan struct{}),
		}
		rf.leaderState.followerState[id] = st
	}
	go func() {
		for {
			select {
			case <-st.stopCh:
				return
			case <-st.triggerCh:
				func() {
					rf.logger.Trace(fmt.Sprintf("id=%s 开始日志追赶", id))
					// 设置状态
					rf.leaderState.setRpcBusy(st.id, true)
					defer rf.leaderState.setRpcBusy(st.id, false)
					// 复制日志，成功后将节点角色提升为 Follower
					replicate := rf.replicate(st)
					rf.logger.Trace(fmt.Sprintf("日志追赶结束，返回值=%t", replicate))
					if replicate && rf.leaderState.followerState[id].role == Learner {
						func() {
							finishCh := make(chan finishMsg)
							defer close(finishCh)
							stopCh := make(chan struct{})
							defer close(stopCh)
							rf.logger.Trace("日志追赶成功，且目标节点是 Learner 角色，发送 EntryPromote 请求")
							rf.replicationTo(id, finishCh, stopCh, EntryPromote)
							msg := <-finishCh
							if msg.msgType == Success {
								rf.leaderState.roleUpgrade(st.id)
								rf.peerState.addPeer(st.id, st.addr)
								rf.logger.Trace("目标节点升级为 Follower 成功")
							}
						}()
					}
				}()
			}
		}
	}()
}

// Follower 和 Candidate 接收到来自 Leader 的 AppendEntries 调用
func (rf *raft) handleCommand(rpcMsg rpc) {

	// 重置选举计时器
	rf.timerState.setElectionTimer()
	rf.logger.Trace("重置选举计时器成功")

	args := rpcMsg.req.(AppendEntry)
	replyRes := AppendEntryReply{}
	var replyErr error
	defer func() {
		rpcMsg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
		rf.logger.Trace("向通道发送返回值成功")
	}()

	// 判断 term
	rfTerm := rf.hardState.currentTerm()
	if args.term < rfTerm {
		// 发送请求的 Leader 任期数落后
		rf.logger.Trace("发送请求的 Leader 任期数落后与本节点")
		replyRes.term = rfTerm
		replyRes.success = false
		return
	}

	// 任期数落后或相等，如果是候选者，需要降级
	// 后续操作都在 Follower / Learner 角色下完成
	stage := rf.roleState.getRoleStage()
	if args.term > rfTerm && stage != Follower && stage != Learner {
		rf.logger.Trace("遇到更大的 term 数，降级为 Follower")
		if !rf.becomeFollower(args.term) {
			replyErr = fmt.Errorf("节点降级失败")
			return
		}
	}

	// 日志一致性检查
	rf.logger.Trace("开始日志一致性检查")
	prevIndex := args.prevLogIndex
	if prevIndex > rf.lastLogIndex() {
		// 当前节点不包含索引为 prevIndex 的日志
		rf.logger.Trace(fmt.Sprintf("当前节点不包含索引为 prevIndex=%d 的日志", prevIndex))
		// 返回最后一个日志条目的 term 及此 term 的首个条目的索引
		logLength := rf.hardState.logLength()
		if logLength <= 0 {
			replyRes.conflictStartIndex = rf.snapshotState.lastIndex()
			replyRes.conflictTerm = rf.snapshotState.lastTerm()
		} else {
			replyRes.conflictTerm = rf.logTerm(logLength - 1)
			replyRes.conflictStartIndex = rf.hardState.lastEntryIndex()
			for i := logLength - 1; i >= 0 && rf.logTerm(i) == replyRes.conflictTerm; i-- {
				replyRes.conflictStartIndex = rf.hardState.logEntry(i).Index
			}
		}
		rf.logger.Trace(fmt.Sprintf("返回最后一个日志条目的 term=%d 及此 term 的首个条目的索引 index=%d",
			replyRes.conflictTerm, replyRes.conflictStartIndex))
		replyRes.term = rfTerm
		replyRes.success = false
		return
	}
	prevTerm := rf.logTerm(prevIndex)
	if prevTerm != args.prevLogTerm {
		// 节点包含索引为 prevIndex 的日志但是 term 数不同
		rf.logger.Trace(fmt.Sprintf("节点包含索引为 prevIndex=%d 的日志但是 args.prevLogTerm=%d, prevLogTerm=%d",
			prevIndex, args.prevLogTerm, prevTerm))
		// 返回 prevIndex 所在 term 及此 term 的首个条目的索引
		replyRes.conflictTerm = prevTerm
		replyRes.conflictStartIndex = prevIndex
		for i := prevIndex - 1; i >= 0 && rf.logTerm(i) == replyRes.conflictTerm; i-- {
			replyRes.conflictStartIndex = rf.hardState.logEntry(i).Index
		}
		rf.logger.Trace(fmt.Sprintf("返回最后一个日志条目的 term=%d 及此 term 的首个条目的索引 index=%d",
			replyRes.conflictTerm, replyRes.conflictStartIndex))
		replyRes.term = rfTerm
		replyRes.success = false
		return
	}

	newEntryIndex := prevIndex + 1
	if args.entryType == EntryReplicate {
		// ========== 接收日志条目 ==========
		rf.logger.Trace("接收到日志条目")
		// 如果当前节点已经有此条目但冲突
		if rf.lastLogIndex() >= newEntryIndex && rf.logTerm(newEntryIndex) != args.term {
			rf.hardState.truncateEntries(prevIndex + 1)
			rf.logger.Trace(fmt.Sprintf("当前节点已经有此条目但冲突，直接覆盖, index=%d, entryIndex=%d, term=%d, entryTerm=%d",
				rf.lastLogIndex(), newEntryIndex, rf.logTerm(newEntryIndex), args.term))
		}

		// 将新条目添加到日志中
		err := rf.addEntry(args.entries[0])
		if err != nil {
			rf.logger.Error(fmt.Errorf("日志添加新条目失败！%w", err).Error())
			replyRes.success = false
		} else {
			replyRes.success = true
		}
		rf.logger.Trace("成功将新条目添加到日志中")
		// 添加日志后不提交，下次心跳来了再提交
		return
	}

	if args.entryType == EntryHeartbeat {
		// ========== 接收心跳 ==========
		rf.logger.Trace("接收到心跳")
		rf.peerState.setLeader(args.leaderId)
		replyRes.term = rf.hardState.currentTerm()

		// 更新提交索引
		leaderCommit := args.leaderCommit
		if leaderCommit > rf.softState.getCommitIndex() {
			var err error
			if leaderCommit >= newEntryIndex {
				rf.softState.setCommitIndex(newEntryIndex)
			} else {
				rf.softState.setCommitIndex(leaderCommit)
			}
			rf.logger.Trace(fmt.Sprintf("成功更新提交索引，commitIndex=%d", rf.softState.getCommitIndex()))
			applyErr := rf.applyFsm()
			if applyErr != nil {
				replyErr = err
				replyRes.success = false
				rf.logger.Trace("日志应用到状态机失败")
			} else {
				replyRes.success = true
				rf.logger.Trace("日志成功应用到状态机")
			}
		}

		// 当日志量超过阈值时，生成快照
		rf.logger.Trace("检查是否需要生成快照")
		rf.checkSnapshot()
		replyRes.success = true
		return
	}

	if args.entryType == EntryChangeConf {
		rf.logger.Trace("接收到成员变更请求")
		configData := args.entries[0].Data
		peerErr := rf.peerState.replacePeersWithBytes(configData)
		if peerErr != nil {
			replyErr = peerErr
			replyRes.success = false
			rf.logger.Trace("新配置应用失败")
		}
		rf.logger.Trace(fmt.Sprintf("新配置应用成功，peers=%v", rf.peerState.peers()))
		replyRes.success = true
		return
	}

	if args.entryType == EntryTimeoutNow {
		rf.logger.Trace("接收到 timeoutNow 请求")
		rf.becomeCandidate()
		rf.logger.Trace("角色成功变为 Candidate")
	}

	// 已接收到全部日志，从 Learner 角色升级为 Follower
	if rf.roleState.getRoleStage() == Learner && args.entryType == EntryPromote {
		rf.logger.Trace(fmt.Sprintf("Learner 接收到升级请求，term=%d", args.term))
		rf.becomeFollower(args.term)
		replyRes.success = true
		rf.logger.Trace("成功升级到Follower")
	}
}

// Follower 和 Candidate 接收到来自 Candidate 的 RequestVote 调用
func (rf *raft) handleVoteReq(rpcMsg rpc) {

	args := rpcMsg.req.(RequestVote)
	replyRes := RequestVoteReply{}
	var replyErr error
	defer func() {
		rpcMsg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
	}()

	rfTerm := rf.hardState.currentTerm()

	if rf.roleState.getRoleStage() == Learner {
		rf.logger.Trace("当前节点是 Learner，不投票")
		replyRes.term = rfTerm
		replyRes.voteGranted = false
	}

	argsTerm := args.term
	if argsTerm < rfTerm {
		// 拉票的候选者任期落后，不投票
		rf.logger.Trace(fmt.Sprintf("拉票的候选者任期落后，不投票。term=%d, args.term=%d", rfTerm, argsTerm))
		replyRes.term = rfTerm
		replyRes.voteGranted = false
		return
	}

	if argsTerm > rfTerm {
		// 角色降级
		needDegrade := rf.roleState.getRoleStage() != Follower
		if needDegrade && !rf.becomeFollower(argsTerm) {
			replyErr = fmt.Errorf("角色降级失败")
			rf.logger.Trace(replyErr.Error())
			return
		}
		rf.logger.Trace(fmt.Sprintf("角色降级成功，argsTerm=%d, currentTerm=%d", argsTerm, rfTerm))
		if !needDegrade  {
			if setTermErr := rf.hardState.setTerm(argsTerm); setTermErr != nil {
				replyErr = fmt.Errorf("设置 term=%d 值失败：%w", argsTerm, setTermErr)
				rf.logger.Trace(replyErr.Error())
				return
			}
		}
	}

	replyRes.term = argsTerm
	replyRes.voteGranted = false
	votedFor := rf.hardState.voted()
	if votedFor == "" || votedFor == args.candidateId {
		// 当前节点是追随者且没有投过票
		rf.logger.Trace("当前节点是追随者且没有投过票，开始比较日志的新旧程度")
		lastIndex := rf.lastLogIndex()
		lastTerm := rf.logTerm(lastIndex)
		// 候选者的日志比当前节点的日志要新，则投票
		// 先比较 term，term 相同则比较日志长度
		if args.lastLogTerm > lastTerm || (args.lastLogTerm == lastTerm && args.lastLogIndex >= lastIndex) {
			rf.logger.Trace(fmt.Sprintf("候选者日志较新，args.lastTerm=%d, lastTerm=%d, args.lastIndex=%d, lastIndex=%d",
				args.lastLogTerm, lastTerm, args.lastLogIndex, lastIndex))
			voteErr := rf.hardState.vote(args.candidateId)
			if voteErr != nil {
				replyErr = fmt.Errorf("更新 votedFor 出错，投票失败：%w", voteErr)
				rf.logger.Error(replyErr.Error())
				replyRes.voteGranted = false
			} else {
				rf.logger.Trace("成功投出一张选票")
				replyRes.voteGranted = true
			}
		} else {
			rf.logger.Trace(fmt.Sprintf("候选者日志不够新，不投票，args.lastTerm=%d, lastTerm=%d, args.lastIndex=%d, lastIndex=%d",
				args.lastLogTerm, lastTerm, args.lastLogIndex, lastIndex))
		}
	}

	if replyRes.voteGranted {
		rf.timerState.setElectionTimer()
		rf.logger.Trace("设置选举计时器成功")
	}
}

// 慢 Follower 接收来自 Leader 的 InstallSnapshot 调用
// 目的是加快日志追赶速度
func (rf *raft) handleSnapshot(rpcMsg rpc) {

	args := rpcMsg.req.(InstallSnapshot)
	replyRes := InstallSnapshotReply{}
	var replyErr error
	defer func() {
		rpcMsg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
	}()

	rfTerm := rf.hardState.currentTerm()
	if args.term < rfTerm {
		// Leader 的 term 过期，直接返回
		rf.logger.Trace("发送快照的 Leader 任期落后，直接返回")
		replyRes.term = rfTerm
		return
	}

	// 持久化
	replyRes.term = rfTerm
	snapshot := Snapshot{
		LastIndex: args.lastIncludedIndex,
		LastTerm: args.lastIncludedTerm,
		Data: args.data,
	}

	saveErr := rf.snapshotState.save(snapshot)
	if saveErr != nil {
		replyErr = fmt.Errorf("持久化快照失败：%w", saveErr)
		return
	}
	rf.logger.Trace("持久化快照成功！")

	if !args.done {
		// 若传送没有完成，则继续接收数据
		return
	}

	// 保存快照成功，删除多余日志
	if args.lastIncludedIndex <= rf.hardState.logLength() && rf.logTerm(args.lastIncludedIndex) == args.lastIncludedTerm {
		rf.logger.Trace("删除快照之前的旧日志")
		rf.hardState.truncateEntries(args.lastIncludedIndex)
		return
	}

	rf.logger.Trace("清空日志")
	rf.hardState.clearEntries()
}

// 处理领导权转移请求
func (rf *raft) handleTransfer(rpcMsg rpc) {
	// 先发送一次心跳，刷新计时器，以及
	args := rpcMsg.req.(TransferLeadership)
	timer := time.NewTimer(rf.timerState.minElectionTimeout())
	// 设置定时器和rpc应答通道
	rf.leaderState.setTransferBusy(args.transferee.id)
	rf.leaderState.setTransferState(timer, rpcMsg.res)
	rf.logger.Trace("成功设置定时器和rpc应答通道")

	// 查看目标节点日志是否最新
	rf.logger.Trace("查看目标节点日志是否最新")
	rf.checkTransfer(args.transferee.id)
}

// 处理客户端请求
func (rf *raft) handleClientCmd(rpcMsg rpc) {

	// 重置心跳计时器
	rf.timerState.setHeartbeatTimer()
	rf.logger.Trace("重置心跳计时器成功")

	args := rpcMsg.req.(ApplyCommand)
	replyRes := ApplyCommandReply{}
	var replyErr error
	defer func() {
		rpcMsg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
	}()

	if !rf.isLeader() {
		rf.logger.Trace("当前节点不是 Leader，请求驳回")
		replyRes = ApplyCommandReply{
			status: NotLeader,
			leader: rf.peerState.getLeader(),
		}
		return
	}

	// Leader 先将日志添加到内存
	rf.logger.Trace("将日志添加到内存")
	addEntryErr := rf.addEntry(Entry{Term: rf.hardState.currentTerm(), Type: EntryReplicate, Data: args.data})
	if addEntryErr != nil {
		replyErr = fmt.Errorf("leader 添加客户端日志失败：%w", addEntryErr)
		rf.logger.Trace(replyErr.Error())
		return
	}

	// 给各节点发送日志条目
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	stopCh := make(chan struct{})
	rf.logger.Trace("给各节点发送日志条目")
	for id := range rf.peerState.peers() {
		// 不用给自己发，正在复制日志的不发
		if rf.peerState.isMe(id) || rf.leaderState.isRpcBusy(id) {
			continue
		}
		// 发送日志
		go rf.replicationTo(id, finishCh, stopCh, EntryReplicate)
	}

	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	success := rf.waitRpcResult(finishCh)
	close(stopCh)
	if !success {
		replyErr = fmt.Errorf("rpc 完成，但日志未复制到多数节点")
		rf.logger.Trace(replyErr.Error())
		return
	}

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目并应用到状态季节
	updateCmtErr := rf.updateLeaderCommit()
	if updateCmtErr != nil {
		replyErr = fmt.Errorf("leader 更新 commitIndex 失败：%w", updateCmtErr)
		rf.logger.Trace(replyErr.Error())
		return
	}

	// 当日志量超过阈值时，生成快照
	rf.logger.Trace("检查是否需要生成快照")
	rf.checkSnapshot()

	replyRes.status = OK
}

// 处理成员变更请求
func (rf *raft) handleConfiguration(msg rpc) {
	newConfig := msg.req.(ChangeConfig)

	// C(new) 配置
	newPeers := newConfig.peers
	rf.leaderState.setNewConfig(newPeers)
	oldPeers := rf.peerState.peers()
	rf.leaderState.setOldConfig(oldPeers)
	rf.logger.Trace(fmt.Sprintf("旧配置：%s，新配置%s", oldPeers, newPeers))

	// C(old,new) 配置
	oldNewPeers := make(map[NodeId]NodeAddr)
	for id, addr := range oldPeers {
		oldNewPeers[id] = addr
	}
	for id, addr := range newPeers {
		oldNewPeers[id] = addr
	}
	rf.logger.Trace(fmt.Sprintf("C(old,new)=%s", oldNewPeers))

	// 分发 C(old,new) 配置
	rf.logger.Trace("分发 C(old,new) 配置")
	if !rf.sendOldNewConfig(oldNewPeers, msg) {
		rf.logger.Trace("C(old,new) 配置分发失败")
		return
	}

	// 分发 C(new) 配置
	rf.logger.Trace("分发 C(new) 配置")
	if !rf.sendConfiguration(newPeers, msg) {
		rf.logger.Trace("C(new) 配置分发失败")
		return
	}

	// 清理 replications
	peers := rf.peerState.peers()
	// 如果当前节点被移除，退出程序
	if _, ok := peers[rf.peerState.myId()]; !ok {
		rf.logger.Trace("新配置中不包含当前节点，程序退出")
		rf.exitCh <- struct{}{}
		return
	}
	// 查看follower有没有被移除的
	rf.logger.Trace("删除新配置中不包含的 replication")
	followers := rf.leaderState.followers()
	for id, f := range followers {
		if _, ok := peers[id]; !ok {
			f.stopCh <- struct{}{}
			delete(followers, id)
		}
	}
}

// 处理添加新节点请求
func (rf *raft) handleNewNode(msg rpc) {
	req := msg.req.(AddNewNode)
	newNode := req.newNode
	// 开启复制循环
	rf.logger.Trace("新空白节点添加到 replication，并触发复制循环")
	rf.addReplication(newNode.id, newNode.addr)
	// 触发复制
	rf.leaderState.followers()[newNode.id].triggerCh <- struct{}{}
}

func (rf *raft) checkSnapshot() {
	go func() {
		if rf.needGenSnapshot() {
			rf.logger.Trace("达成生成快照的条件")
			data, serializeErr := rf.fsm.Serialize()
			if serializeErr != nil {
				rf.logger.Error(fmt.Errorf("状态机生成快照失败！%w", serializeErr).Error())
			}
			rf.logger.Trace("状态机生成快照成功")
			newSnapshot := Snapshot{
				LastIndex: rf.softState.lastApplied,
				LastTerm: rf.hardState.currentTerm(),
				Data: data,
			}
			saveErr := rf.snapshotState.save(newSnapshot)
			if saveErr != nil {
				rf.logger.Error(fmt.Errorf("保存快照失败！%w", serializeErr).Error())
			}
			rf.logger.Trace("持久化快照成功")
		}
	}()
}

func (rf *raft) checkTransfer(id NodeId) {
	select {
	case <-rf.leaderState.transfer.timer.C:
		rf.leaderState.setTransferBusy(None)
	default:
		if rf.leaderState.isRpcBusy(id) {
			// 若目标节点正在复制日志，则继续等待
			return
		}
		if rf.leaderState.matchIndex(id) == rf.lastLogIndex() {
			// 目标节点日志已是最新，发送 timeoutNow 消息
			args := AppendEntry{entryType: EntryTimeoutNow}
			res := &AppendEntryReply{}
			err := rf.transport.AppendEntries(rf.peerState.peers()[id], args, res)
			reply := rf.leaderState.transfer.reply
			if err != nil {
				reply <- rpcReply{err: err}
				return
			}
			term := rf.hardState.currentTerm()
			if res.term > term {
				term = res.term
				reply <- rpcReply{err: fmt.Errorf("term 落后，角色降级")}
			} else {
				reply <- rpcReply{res: res}
			}
			rf.becomeFollower(term)
			rf.leaderState.setTransferBusy(None)
		} else {
			// 目标节点不是最新，开始日志复制
			rf.leaderState.followerState[id].triggerCh <- struct{}{}
		}
	}
}

func (rf *raft) sendOldNewConfig(peers map[NodeId]NodeAddr, msg rpc) bool {
	oldNewPeersData, enOldNewErr := encodePeersMap(peers)
	if enOldNewErr != nil {
		msg.res <- rpcReply{err: enOldNewErr}
		return false
	}

	// C(old,new)配置添加到状态
	addEntryErr := rf.addEntry(Entry{Type: EntryChangeConf, Data: oldNewPeersData})
	if addEntryErr != nil {
		msg.res <- rpcReply{err: addEntryErr}
		return false
	}
	rf.peerState.replacePeers(peers)

	// C(old,new)发送到各个节点
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	stopCh := make(chan struct{})
	defer close(stopCh)

	// 先给旧节点发，再给新节点发
	return rf.waitForConfig(rf.leaderState.getOldConfig(), finishCh, stopCh, msg) &&
		rf.waitForConfig(rf.leaderState.getNewConfig(), finishCh, stopCh, msg)
}

func (rf *raft) sendConfiguration(peers map[NodeId]NodeAddr, msg rpc) bool {

	oldNewPeersData, enOldNewErr := encodePeersMap(peers)
	if enOldNewErr != nil {
		msg.res <- rpcReply{err: enOldNewErr}
		return false
	}

	// C(old,new)配置添加到状态
	addEntryErr := rf.addEntry(Entry{Type: EntryChangeConf, Data: oldNewPeersData})
	if addEntryErr != nil {
		msg.res <- rpcReply{err: addEntryErr}
		return false
	}
	rf.peerState.replacePeers(peers)

	// C(old,new)发送到各个节点
	finishCh := make(chan finishMsg)
	defer close(finishCh)
	stopCh := make(chan struct{})
	defer close(stopCh)
	for id := range rf.peerState.peers() {
		// 不用给自己发
		if rf.peerState.isMe(id) {
			continue
		}
		// 发送日志
		go rf.replicationTo(id, finishCh, stopCh, EntryChangeConf)
	}

	count := 1
	successCnt := 1
	for result := range finishCh {
		if result.msgType == Degrade && rf.becomeFollower(result.term) {
			return false
		}
		if result.msgType == Success {
			successCnt += 1
		}
		count += 1
		if successCnt >= rf.peerState.majority() {
			break
		}
		if count >= rf.peerState.peersCnt() {
			msg.res <- rpcReply{err: fmt.Errorf("日志未发送到多数节点")}
			return false
		}
	}

	// 提交日志
	oldNewIndex := rf.lastLogIndex()
	rf.softState.setCommitIndex(oldNewIndex)
	return true
}

func (rf *raft) waitForConfig(peers map[NodeId]NodeAddr, finishCh chan finishMsg, stopCh chan struct{}, msg rpc) bool {

	for id := range peers {
		// 不用给自己发
		if rf.peerState.isMe(id) {
			continue
		}
		// 发送日志
		go rf.replicationTo(id, finishCh, stopCh, EntryChangeConf)
	}

	count := 1
	successCnt := 1
	for result := range finishCh {
		if result.msgType == Degrade && rf.becomeFollower(result.term) {
			return false
		}
		if result.msgType == Success {
			successCnt += 1
		}
		count += 1
		if successCnt >= rf.peerState.majority() {
			break
		}
		if count >= rf.peerState.peersCnt() {
			msg.res <- rpcReply{err: fmt.Errorf("日志未发送到多数节点")}
			return false
		}
	}

	// 提交日志
	oldNewIndex := rf.lastLogIndex()
	rf.softState.setCommitIndex(oldNewIndex)
	return true
}

func encodePeersMap(peers map[NodeId]NodeAddr) ([]byte, error) {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)
	enErr := encoder.Encode(peers)
	if enErr != nil {
		return nil, enErr
	}
	return data.Bytes(), nil
}

// Leader 给某个节点发送心跳/日志
func (rf *raft) replicationTo(id NodeId, finishCh chan finishMsg, stopCh chan struct{}, entryType EntryType) {
	var msg finishMsg
	defer func() {
		if _, ok := <-stopCh; !ok {
			finishCh <- msg
		}
	}()

	// 发起 RPC 调用
	addr := rf.peerState.peers()[id]
	prevIndex := rf.leaderState.nextIndex(id) - 1
	var entries []Entry
	if entryType != EntryHeartbeat && entryType != EntryPromote {
		entries = rf.hardState.logEntries(rf.hardState.lastEntryIndex(), rf.hardState.logLength())
	}
	args := AppendEntry{
		entryType:    entryType,
		term:         rf.hardState.currentTerm(),
		leaderId:     rf.peerState.myId(),
		prevLogIndex: prevIndex,
		prevLogTerm:  rf.logTerm(prevIndex),
		entries:      entries,
		leaderCommit: rf.softState.getCommitIndex(),
	}
	res := &AppendEntryReply{}
	err := rf.transport.AppendEntries(addr, args, res)

	// 处理 RPC 调用结果
	if err != nil {
		rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err).Error())
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
	} else if entryType != EntryChangeConf {
		// Follower 和 Leader 的日志不匹配，进行日志追赶
		rf.leaderState.followerState[id].triggerCh <- struct{}{}
		msg = finishMsg{msgType: Success}
	}
}

// 给指定节点发送最新日志
// 若日志不同步，开始进行日志追赶操作
// 1. Follower 节点标记为日志追赶状态，下一次心跳时跳过此节点
// 2. 日志追赶完毕或 rpc 调用失败，Follower 节点标记为普通状态
func (rf *raft) replicate(s *Replication) bool {
	// 向前查找 nextIndex 值
	if rf.findCorrectNextIndex(s) {
		// 递增更新 matchIndex 值
		return rf.completeEntries(s)
	}
	return false
}

func (rf *raft) findCorrectNextIndex(s *Replication) bool {
	rl := rf.leaderState
	peerNextIndex := rl.nextIndex(s.id)

	for peerNextIndex >= 0 {
		prevIndex := rl.nextIndex(s.id) - 1

		// 找到匹配点之前，发送空日志节省带宽
		var entries []Entry
		if rl.matchIndex(s.id) == prevIndex {
			rf.hardState.logEntries(prevIndex, prevIndex+1)
		}
		args := AppendEntry{
			term:         rf.hardState.currentTerm(),
			leaderId:     rf.peerState.myId(),
			prevLogIndex: prevIndex,
			prevLogTerm:  rf.logTerm(prevIndex),
			leaderCommit: rf.softState.getCommitIndex(),
			entries:      entries,
		}
		res := &AppendEntryReply{}
		err := rf.transport.AppendEntries(s.addr, args, res)

		// 确保下面对操作在 Leader 角色下完成
		select {
		case <-s.stopCh:
			return false
		default:
		}

		if err != nil {
			rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", s.addr, err).Error())
			return false
		}
		if res.term > rf.hardState.currentTerm() && !rf.becomeFollower(res.term) {
			// 如果任期数小，降级为 Follower
			return false
		}
		if res.success {
			return true
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
		rl.setNextIndex(s.id, conflictStartIndex)
	}
	return true
}

func (rf *raft) completeEntries(s *Replication) bool {

	rl := rf.leaderState
	for rl.nextIndex(s.id)-1 != rf.lastLogIndex() {
		// 缺失的日志太多时，直接发送快照
		snapshot := rf.snapshotState.getSnapshot()
		finishCh := make(chan finishMsg)
		if rl.nextIndex(s.id) <= snapshot.LastIndex {
			rf.snapshotTo(s.id, s.addr, snapshot.Data, finishCh, make(chan struct{}))
			msg := <-finishCh
			if msg.msgType != Success {
				if msg.msgType == Degrade && rf.becomeFollower(msg.term) {
					return false
				}
			}

			select {
			case <-s.stopCh:
				return false
			default:
			}

			rf.leaderState.setMatchAndNextIndex(s.id, snapshot.LastIndex, snapshot.LastIndex+1)
		}

		prevIndex := rl.nextIndex(s.id) - 1
		args := AppendEntry{
			term:         rf.hardState.currentTerm(),
			leaderId:     rf.peerState.myId(),
			prevLogIndex: prevIndex,
			prevLogTerm:  rf.logTerm(prevIndex),
			leaderCommit: rf.softState.getCommitIndex(),
			entries:      rf.hardState.logEntries(prevIndex, rl.nextIndex(s.id)),
		}
		res := &AppendEntryReply{}
		rpcErr := rf.transport.AppendEntries(s.addr, args, res)

		select {
		case <-s.stopCh:
			return false
		default:
		}

		if rpcErr != nil {
			rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", s.addr, rpcErr).Error())
			return false
		}
		if res.term > rf.hardState.currentTerm() && rf.becomeFollower(res.term) {
			// 如果任期数小，降级为 Follower
			return false
		}

		// 向后补充
		rf.leaderState.setMatchAndNextIndex(s.id, rl.nextIndex(s.id), rl.nextIndex(s.id)+1)
	}
	return true
}

func (rf *raft) snapshotTo(id NodeId, addr NodeAddr, data []byte, finishCh chan finishMsg, stopCh chan struct{}) {
	var msg finishMsg
	defer func() {
		select {
		case <-stopCh:
		default:
			finishCh <- msg
		}
	}()
	args := InstallSnapshot{
		term:              rf.hardState.currentTerm(),
		leaderId:          rf.peerState.myId(),
		lastIncludedIndex: rf.softState.getCommitIndex(),
		lastIncludedTerm:  rf.logTerm(rf.softState.getCommitIndex()),
		offset:            0,
		data:              data,
		done:              true,
	}
	res := &InstallSnapshotReply{}
	err := rf.transport.InstallSnapshot(addr, args, res)
	if err != nil {
		rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err).Error())
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
	stopCh := make(chan struct{})
	for id := range rf.peerState.peers() {
		if rf.peerState.isMe(id) {
			continue
		}

		go rf.replicationTo(id, finishCh, stopCh, EntryHeartbeat)
	}

	// 权柄建立成功，将自己置为 Leader
	if rf.waitRpcResult(finishCh) {
		rf.setRoleStage(Leader)
		rf.peerState.setLeader(rf.peerState.myId())
		return true
	}
	return false
}

func (rf *raft) becomeCandidate() bool {
	// 重置选举计时器
	rf.timerState.setElectionTimer()
	// 角色置为候选者
	rf.setRoleStage(Candidate)
	return true
}

// 降级为 Follower
func (rf *raft) becomeFollower(term int) bool {

	err := rf.hardState.setTerm(term)
	if err != nil {
		rf.logger.Error(fmt.Errorf("降级失败%w", err).Error())
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
	commitIndex := rf.softState.getCommitIndex()
	lastApplied := rf.softState.getLastApplied()

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
	peers := rf.peerState.peers()
	//
	for id := range peers {
		indexCnt[rf.leaderState.matchIndex(id)] = 1
	}

	// 计算出多少个节点有相同的 matchIndex 值
	for index := range indexCnt {
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

	if rf.softState.getCommitIndex() < maxMajorityMatch {
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

func (rf *raft) needGenSnapshot() bool {
	return rf.softState.getCommitIndex()-rf.snapshotState.lastIndex() >= rf.snapshotState.logThreshold()
}
