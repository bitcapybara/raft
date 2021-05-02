package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

type finishMsgType uint8

const (
	Error finishMsgType = iota
	RpcFailed
	Degrade
	Success
)

type finishMsg struct {
	msgType finishMsgType
	term    int
	id      NodeId
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

	// 应用快照数据
	Install([]byte) error
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

	roleObserver []chan RoleStage // 节点角色变更观察者
	obMu         sync.Mutex
}

func newRaft(config Config) *raft {
	if config.ElectionMinTimeout > config.ElectionMaxTimeout {
		panic("ElectionMinTimeout 不能大于 ElectionMaxTimeout！")
	}
	// 加载快照
	var snpshtState snapshotState
	snpshtPersister := config.SnapshotPersister
	if snpshtPersister != nil {
		snapshot, snapshotErr := snpshtPersister.LoadSnapshot()
		if snapshotErr != nil {
			log.Fatalln(fmt.Errorf("加载快照失败：%w", snapshotErr))
		}
		snpshtState = snapshotState{
			snapshot:     &snapshot,
			persister:    snpshtPersister,
			maxLogLength: config.MaxLogLength,
		}
	} else {
		log.Fatalln("缺失 SnapshotPersister!")
	}

	// 加载 hardState
	raftPst := config.RaftStatePersister
	var raftState RaftState
	if raftPst != nil {
		rfState, raftStateErr := raftPst.LoadRaftState()
		if raftStateErr != nil {
			panic(fmt.Sprintf("持久化器加载 RaftState 失败：%s\n", raftStateErr))
		} else {
			raftState = rfState
		}
	} else {
		log.Fatalln("缺失 RaftStatePersister!")
	}
	hardState := raftState.toHardState(raftPst)

	// 如果是初次加载
	if snpshtState.snapshot.LastIndex <= 0 && len(hardState.entries) <= 0 {
		hardState.entries = make([]Entry, 1)
	}

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
		snapshotState: &snpshtState,
		rpcCh:         make(chan rpc),
		exitCh:        make(chan struct{}),
	}
}

func (rf *raft) raftRun(rpcCh chan rpc) {
	rf.rpcCh = rpcCh
	go func() {
		for {
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

	go func() {
		<-rf.exitCh
		rf.logger.Trace("接收到程序退出信号")
		rf.timerState.stopTimer()
		os.Exit(0)
	}()
}

func (rf *raft) runLeader() {
	rf.logger.Trace("进入 runLeader()")
	// 初始化心跳定时器
	rf.timerState.setHeartbeatTimer()
	rf.logger.Trace("初始化心跳定时器成功")

	// 开启日志复制循环
	rf.runReplication()
	rf.logger.Trace("已开启全部节点日志复制循环")

	// 节点退出 Leader 状态，收尾工作
	defer func() {
		for _, st := range rf.leaderState.replications {
			close(st.stopCh)
		}
		rf.logger.Trace("退出 runLeader()，关闭各个 replication 的 stopCh")
	}()

	for rf.roleState.getRoleStage() == Leader {
		select {
		case msg := <-rf.rpcCh:
			if transfereeId, busy := rf.leaderState.isTransferBusy(); busy {
				// 如果正在进行领导权转移
				rf.logger.Trace("节点正在进行领导权转移，请求驳回！")
				msg.res <- rpcReply{err: fmt.Errorf("正在进行领导权转移，请求驳回！")}
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
					rf.handleConfigChange(msg)
				case TransferLeadershipRpc:
					rf.logger.Trace("接收到 TransferLeadershipRpc 请求")
					rf.handleTransfer(msg)
				case AddLearnerRpc:
					rf.logger.Trace("接收到 AddLearnerRpc 请求")
					rf.handleLearnerAdd(msg)
				}
			}
		case <-rf.timerState.tick():
			rf.logger.Trace("心跳计时器到期，开始发送心跳")
			stopCh := make(chan struct{})
			finishCh := rf.heartbeat(stopCh)
			successCnt := 0
			count := 0
			end := false
			after := time.After(rf.timerState.heartbeatDuration())
			for !end {
				select {
				case <-after:
					rf.logger.Trace("操作超时退出")
					end = true
				case msg := <-finishCh:
					if msg.msgType == Degrade && rf.becomeFollower(msg.term) {
						rf.logger.Trace("降级为 Follower")
						end = true
						break
					}
					nodeId := msg.id
					if msg.msgType == Success {
						rf.logger.Trace(fmt.Sprintf("获取到 id=%s 的心跳结果：Success", nodeId))
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

	successCnt := 0
	for rf.roleState.getRoleStage() == Candidate {
		select {
		case <-rf.timerState.tick():
			// 开启下一轮选举
			rf.logger.Trace("选举计时器到期，开启新一轮选举")
			return
		case msg := <-rf.rpcCh:
			switch msg.rpcType {
			case ApplyCommandRpc:
				rf.logger.Trace("当前节点不是 Leader，ApplyCommandRpc 请求驳回")
				replyRes := ApplyCommandReply{
					Status: NotLeader,
					Leader: rf.peerState.getLeader(),
				}
				msg.res <- rpcReply{res: replyRes}
			case AppendEntryRpc:
				rf.logger.Trace("接收到 AppendEntryRpc 请求")
				rf.handleCommand(msg)
			case RequestVoteRpc:
				rf.logger.Trace("接收到 RequestVoteRpc 请求")
				rf.handleVoteReq(msg)
			case InstallSnapshotRpc:
				rf.logger.Trace("接收到 RequestVoteRpc 请求")
				rf.handleSnapshot(msg)
			case ChangeConfigRpc:
				rf.logger.Trace("当前节点不是 Leader，ChangeConfigRpc 请求驳回")
				replyRes := ChangeConfigReply{
					Status: NotLeader,
					Leader: rf.peerState.getLeader(),
				}
				msg.res <- rpcReply{res: replyRes}
			case AddLearnerRpc:
				rf.logger.Trace("当前节点不是 Leader，AddLearnerRpc 请求驳回")
				replyRes := AddLearnerReply{
					Status: NotLeader,
					Leader: rf.peerState.getLeader(),
				}
				msg.res <- rpcReply{res: replyRes}
			}
		case msg := <-finishCh:
			// 降级
			if msg.msgType == Error {
				break
			}
			if msg.msgType == Degrade && rf.becomeFollower(msg.term) {
				rf.logger.Trace("降级为 Follower")
				return
			}
			if msg.msgType == Success {
				successCnt += 1
			}
			// 升级
			if successCnt >= rf.peerState.majority() {
				rf.logger.Trace("获取到多数节点投票")
				if rf.becomeLeader() {
					rf.logger.Trace("升级为 Leader")
				}
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
		case msg := <-rf.rpcCh:
			switch msg.rpcType {
			case ApplyCommandRpc:
				rf.logger.Trace("当前节点不是 Leader，ApplyCommandRpc 请求驳回")
				replyRes := ApplyCommandReply{
					Status: NotLeader,
					Leader: rf.peerState.getLeader(),
				}
				msg.res <- rpcReply{res: replyRes}
			case AppendEntryRpc:
				rf.logger.Trace("接收到 AppendEntryRpc 请求")
				rf.handleCommand(msg)
			case RequestVoteRpc:
				rf.logger.Trace("接收到 RequestVoteRpc 请求")
				rf.handleVoteReq(msg)
			case InstallSnapshotRpc:
				rf.logger.Trace("接收到 InstallSnapshotRpc 请求")
				rf.handleSnapshot(msg)
			case ChangeConfigRpc:
				rf.logger.Trace("当前节点不是 Leader，ChangeConfigRpc 请求驳回")
				replyRes := ChangeConfigReply{
					Status: NotLeader,
					Leader: rf.peerState.getLeader(),
				}
				msg.res <- rpcReply{res: replyRes}
			case AddLearnerRpc:
				rf.logger.Trace("当前节点不是 Leader，AddLearnerRpc 请求驳回")
				replyRes := AddLearnerReply{
					Status: NotLeader,
					Leader: rf.peerState.getLeader(),
				}
				msg.res <- rpcReply{res: replyRes}
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

	for id, addr := range rf.peerState.peers() {
		if rf.peerState.isMe(id) {
			rf.logger.Trace(fmt.Sprintf("自身节点，不发送心跳。Id=%s", id))
			go func() { finishCh <- finishMsg{msgType: Success, id: id} }()
			continue
		}
		if rf.leaderState.isRpcBusy(id) {
			rf.logger.Trace(fmt.Sprintf("忙节点，不发送心跳。Id=%s", id))
			go func() { finishCh <- finishMsg{msgType: Error} }()
			continue
		}
		rf.logger.Trace(fmt.Sprintf("给 Id=%s 的节点发送心跳", id))
		go rf.replicationTo(id, addr, finishCh, stopCh, EntryHeartbeat)
	}

	return finishCh
}

// Candidate / Follower 开启新一轮选举
func (rf *raft) election(stopCh chan struct{}) <-chan finishMsg {
	// pre-vote
	preVoteFinishCh := rf.sendRequestVote(stopCh, true)

	finish := false
	count := 0
	successCnt := 0
	end := false
	after := time.After(rf.timerState.heartbeatDuration())
	for !end {
		select {
		case <-after:
			rf.logger.Trace("操作超时退出")
			end = true
		case msg := <-preVoteFinishCh:
			if msg.msgType == Degrade {
				rf.logger.Trace("接收到降级请求")
				if rf.becomeFollower(msg.term) {
					rf.logger.Trace("降级成功")
				}
				end = true
				break
			}
			if msg.msgType == Success {
				rf.logger.Trace("接收到成功响应")
				successCnt += 1
			}
			if successCnt >= rf.peerState.majority() {
				rf.logger.Trace("投票请求已成功发送给多数节点")
				end = true
				finish = true
			}
			count += 1
			if count >= rf.peerState.peersCnt() {
				rf.logger.Trace("已接收所有响应，成功节点数未达到多数")
				end = true
			}
		}
	}

	if !finish {
		rf.logger.Trace("preVote 失败，退出选举")
		go func() { preVoteFinishCh <- finishMsg{msgType: Error} }()
		return preVoteFinishCh
	}

	// 增加 Term 数
	err := rf.hardState.termAddAndVote(1, rf.peerState.myId())
	if err != nil {
		rf.logger.Error(fmt.Errorf("增加term，设置votedFor失败%w", err).Error())
	}
	rf.logger.Trace(fmt.Sprintf("增加 Term 数，开始发送 RequestVote 请求。Term=%d", rf.hardState.currentTerm()))

	return rf.sendRequestVote(stopCh, false)
}

func (rf *raft) sendRequestVote(stopCh <-chan struct{}, isPreVote bool) chan finishMsg {
	// 发送 RV 请求
	finishCh := make(chan finishMsg)

	args := RequestVote{
		IsPreVote:   isPreVote,
		Term:        rf.hardState.currentTerm(),
		CandidateId: rf.peerState.myId(),
	}
	for id, addr := range rf.peerState.peers() {
		if rf.peerState.isMe(id) {
			rf.logger.Trace(fmt.Sprintf("自身节点，不发送投票请求。Id=%s", id))
			go func() { finishCh <- finishMsg{msgType: Success} }()
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

			if res.VoteGranted {
				// 成功获得选票
				rf.logger.Trace(fmt.Sprintf("成功获得来自 Id=%s 的选票", id))
				msg = finishMsg{msgType: Success}
				return
			}

			term := rf.hardState.currentTerm()
			if res.Term > term {
				// 当前任期数落后，降级为 Follower
				rf.logger.Trace(fmt.Sprintf("当前任期数落后，降级为 Follower, Term=%d, resTerm=%d", term, res.Term))
				msg = finishMsg{msgType: Degrade, term: res.Term}
			}
		}(id, addr)
	}

	return finishCh
}

func (rf *raft) runReplication() {
	for id, addr := range rf.peerState.peers() {
		if replication, ok := rf.leaderState.replications[id]; ok || rf.peerState.isMe(id) {
			continue
		} else {
			rf.logger.Trace(fmt.Sprintf("生成节点 Id=%s 的 Replication 对象", id))
			replication = rf.newReplication(id, addr, Follower)
			rf.leaderState.replications[id] = replication
			rf.logger.Trace(fmt.Sprintf("开启复制循环：id=%s", id))
			go rf.addReplication(replication)
		}
	}
}

func (rf *raft) newReplication(id NodeId, addr NodeAddr, role RoleStage) *Replication {
	return &Replication{
		id:         id,
		addr:       addr,
		role:       role,
		nextIndex:  rf.lastEntryIndex() + 1,
		matchIndex: 0,
		stepDownCh: rf.leaderState.stepDownCh,
		stopCh:     make(chan struct{}),
		triggerCh:  make(chan struct{}),
	}
}

func (rf *raft) addReplication(r *Replication) {
	for {
		select {
		case <-r.stopCh:
			rf.logger.Trace(fmt.Sprintf("退出复制循环：id=%s", r.id))
			delete(rf.leaderState.replications, r.id)
			return
		case <-r.triggerCh:
			func() {
				rf.logger.Trace(fmt.Sprintf("Id=%s 开始日志追赶", r.id))
				// 设置状态
				rf.leaderState.setRpcBusy(r.id, true)
				defer rf.leaderState.setRpcBusy(r.id, false)
				// 复制日志
				replicate := rf.replicate(r)
				rf.logger.Trace(fmt.Sprintf("日志追赶结束，返回值=%t", replicate))
				if replicate {
					rf.updateLeaderCommit()
					rf.logger.Trace(fmt.Sprintf("commitIndex 更新为 %d", rf.softState.getCommitIndex()))
				}
			}()
		}
	}
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
	}()

	// 判断 Term
	rfTerm := rf.hardState.currentTerm()
	if args.Term < rfTerm {
		// 发送请求的 Leader 任期数落后
		rf.logger.Trace("发送请求的 Leader 任期数落后于本节点")
		replyRes.Term = rfTerm
		replyRes.Success = false
		return
	}

	// 任期数落后或相等，如果是候选者，需要降级
	// 后续操作都在 Follower / Learner 角色下完成
	stage := rf.roleState.getRoleStage()
	if args.Term > rfTerm && stage != Follower && stage != Learner {
		rf.logger.Trace("遇到更大的 Term 数，降级为 Follower")
		if !rf.becomeFollower(args.Term) {
			replyErr = fmt.Errorf("节点降级失败")
			rf.logger.Error(replyErr.Error())
			return
		}
	}
	if termErr := rf.hardState.setTerm(args.Term); termErr != nil {
		replyErr = fmt.Errorf("节点设置 term 值失败！")
		rf.logger.Error(replyErr.Error())
		return
	}

	// 日志一致性检查
	rf.logger.Trace("开始日志一致性检查")
	prevIndex := args.PrevLogIndex
	if prevIndex > rf.lastEntryIndex() {
		rf.logger.Trace("当前节点不包含 prevLog ")
		func() {
			defer func() {
				rf.logger.Trace(fmt.Sprintf("返回最后一个日志条目的 Term=%d 及此 Term 的首个条目的索引 index=%d",
					replyRes.ConflictTerm, replyRes.ConflictStartIndex))
				replyRes.Term = rfTerm
				replyRes.Success = false
			}()
			// 当前节点不包含索引为 prevIndex 的日志
			rf.logger.Trace(fmt.Sprintf("当前节点不包含索引为 prevIndex=%d 的日志", prevIndex))
			// 返回最后一个日志条目的 Term 及此 Term 的首个条目的索引
			replyRes.ConflictTerm = rf.lastEntryTerm()
			replyRes.ConflictStartIndex = rf.lastEntryIndex()
			for i := rf.lastEntryIndex() - 1; i >= 0; i-- {
				if !rf.entryExist(i) {
					break
				}
				if iEntry, iEntryErr := rf.logEntry(i); iEntryErr != nil {
					rf.logger.Error(iEntryErr.Error())
					replyRes.ConflictStartIndex = 0
					break
				} else if iEntry.Term == replyRes.ConflictTerm {
					replyRes.ConflictStartIndex = iEntry.Index
				} else {
					rf.logger.Trace(fmt.Sprintf("第 %d 日志term %d != conflictTerm", i, iEntry.Term))
					break
				}
			}
		}()
		return
	}
	prevEntry, prevEntryErr := rf.logEntry(prevIndex)
	if prevEntryErr != nil {
		replyErr = fmt.Errorf("获取 index=%d 的日志失败！%w", prevIndex, prevEntryErr)
		rf.logger.Error(replyErr.Error())
		return
	}
	if prevTerm := prevEntry.Term; prevTerm != args.PrevLogTerm {
		func() {
			defer func() {
				rf.logger.Trace(fmt.Sprintf("返回最后一个日志条目的 Term=%d 及此 Term 的首个条目的索引 index=%d",
					replyRes.ConflictTerm, replyRes.ConflictStartIndex))
				replyRes.Term = rfTerm
				replyRes.Success = false
			}()
			// 节点包含索引为 prevIndex 的日志但是 Term 数不同
			rf.logger.Trace(fmt.Sprintf("节点包含索引为 prevIndex=%d 的日志但是 args.PrevLogTerm=%d, PrevLogTerm=%d",
				prevIndex, args.PrevLogTerm, prevTerm))
			// 返回 prevIndex 所在 Term 及此 Term 的首个条目的索引
			replyRes.ConflictTerm = prevTerm
			replyRes.ConflictStartIndex = prevIndex
			for i := prevIndex - 1; i >= 0; i-- {
				if !rf.entryExist(i) {
					break
				}
				if iEntry, iEntryErr := rf.logEntry(i); iEntryErr != nil {
					rf.logger.Error(iEntryErr.Error())
					replyRes.ConflictStartIndex = 0
					break
				} else if iEntry.Term == replyRes.ConflictTerm {
					replyRes.ConflictStartIndex = iEntry.Index
				} else {
					rf.logger.Trace(fmt.Sprintf("第 %d 日志term %d != conflictTerm", i, iEntry.Term))
					break
				}
			}
		}()
		return
	}
	rf.logger.Trace("日志一致性检查通过")

	newEntryIndex := prevIndex + 1
	replyRes.Term = rfTerm
	replyRes.Success = true
	if args.EntryType == EntryReplicate {
		// ========== 接收日志条目 ==========
		rf.logger.Trace("接收到日志条目")
		// 如果当前节点已经有此条目
		if rf.lastEntryIndex() >= newEntryIndex {
			rf.logger.Trace(fmt.Sprintf("当前节点已经含有 index=%d 的日志", newEntryIndex))
			if entry, entryErr := rf.logEntry(newEntryIndex); entryErr != nil {
				replyErr = fmt.Errorf("获取 index=%d 的日志失败！%w", newEntryIndex, entryErr)
				rf.logger.Error(replyErr.Error())
				return
			} else if entry.Term != args.Term {
				rf.logger.Trace(fmt.Sprintf("当前节点 index=%d 的日志与新条目冲突。term=%d, args.term=%d，截断之后的日志",
					newEntryIndex, entry.Term, args.Term))
				truncateErr := rf.truncateAfter(newEntryIndex)
				if truncateErr != nil {
					replyErr = fmt.Errorf("截断日志失败！%w", truncateErr)
					rf.logger.Error(replyErr.Error())
					return
				}
				rf.logger.Trace("日志截断成功！")
				// 将新条目添加到日志中
				err := rf.addEntry(args.Entries[0])
				if err != nil {
					replyErr = fmt.Errorf("日志添加新条目失败！%w", err)
					rf.logger.Error(replyErr.Error())
					return
				}
				rf.logger.Trace("成功将新条目添加到日志中")
			} else {
				rf.logger.Trace("当前节点已包含新日志")
			}
		} else {
			// 将新条目添加到日志中
			err := rf.addEntry(args.Entries[0])
			if err != nil {
				replyErr = fmt.Errorf("日志添加新条目失败！%w", err)
				rf.logger.Error(replyErr.Error())
				return
			}
			rf.logger.Trace("成功将新条目添加到日志中")
		}

		// 更新提交索引
		leaderCommit := args.LeaderCommit
		if leaderCommit > rf.softState.getCommitIndex() {
			lastEntryIndex := rf.lastEntryIndex()
			if leaderCommit >= rf.lastEntryIndex() {
				rf.softState.setCommitIndex(lastEntryIndex)
			} else {
				rf.softState.setCommitIndex(leaderCommit)
			}
			rf.logger.Trace(fmt.Sprintf("成功更新提交索引，commitIndex=%d", rf.softState.getCommitIndex()))
			applyErr := rf.applyFsm()
			if applyErr != nil {
				rf.logger.Error(fmt.Errorf("日志应用到状态机失败！%w", applyErr).Error())
			} else {
				rf.logger.Trace("日志成功应用到状态机")
			}
		}

		// 当日志量超过阈值时，生成快照
		rf.logger.Trace("检查是否需要生成快照")
		rf.updateSnapshot()

		return
	}

	if args.EntryType == EntryHeartbeat {
		// ========== 接收心跳 ==========
		rf.logger.Trace("接收到心跳")
		rf.peerState.setLeader(args.LeaderId)
		replyRes.Term = rf.hardState.currentTerm()

		// 更新提交索引
		if prevIndex > rf.softState.getCommitIndex() {
			rf.softState.setCommitIndex(prevIndex)
			rf.logger.Trace(fmt.Sprintf("成功更新提交索引，commitIndex=%d", rf.softState.getCommitIndex()))
			applyErr := rf.applyFsm()
			if applyErr != nil {
				rf.logger.Error(fmt.Errorf("日志应用到状态机失败！%w", applyErr).Error())
			} else {
				rf.logger.Trace("日志成功应用到状态机")
			}
		}

		// 当日志量超过阈值时，生成快照
		rf.logger.Trace("检查是否需要生成快照")
		rf.updateSnapshot()
		return
	}

	if args.EntryType == EntryChangeConf {
		rf.logger.Trace("接收到成员变更请求")
		configData := args.Entries[0].Data
		peerErr := rf.peerState.replacePeersWithBytes(configData)
		if peerErr != nil {
			replyErr = peerErr
			replyRes.Success = false
			rf.logger.Trace("新配置应用失败")
		}
		rf.logger.Trace(fmt.Sprintf("新配置应用成功，Peers=%+v", rf.peerState.peers()))
		if _, ok := rf.peerState.peers()[rf.peerState.myId()]; !ok {
			rf.logger.Trace("新配置中不包含当前节点，退出程序")
			go func() { rf.exitCh <- struct{}{} }()
			return
		}
		replyRes.Success = true
		return
	}

	if args.EntryType == EntryTimeoutNow {
		rf.logger.Trace("接收到 timeoutNow 请求")
		replyRes.Success = rf.becomeCandidate()
		if replyRes.Success {
			rf.logger.Trace("角色成功变为 Candidate")
		} else {
			rf.logger.Trace("角色变为候选者失败")
		}
		return
	}

	// 已接收到全部日志，从 Learner 角色升级为 Follower
	if rf.roleState.getRoleStage() == Learner && args.EntryType == EntryPromote {
		rf.logger.Trace(fmt.Sprintf("Learner 接收到升级请求，Term=%d", args.Term))
		replyRes.Success = rf.becomeFollower(args.Term)
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

	rf.logger.Trace(fmt.Sprintf("接收到的参数：%+v", args))
	rfTerm := rf.hardState.currentTerm()

	if rf.roleState.getRoleStage() == Learner {
		rf.logger.Trace("当前节点是 Learner，不投票")
		replyRes.Term = rfTerm
		replyRes.VoteGranted = false
	}

	argsTerm := args.Term
	if argsTerm < rfTerm {
		// 拉票的候选者任期落后，不投票
		rf.logger.Trace(fmt.Sprintf("拉票的候选者任期落后，不投票。Term=%d, args.Term=%d", rfTerm, argsTerm))
		replyRes.Term = rfTerm
		replyRes.VoteGranted = false
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
		if !needDegrade {
			if setTermErr := rf.hardState.setTerm(argsTerm); setTermErr != nil {
				replyErr = fmt.Errorf("设置 Term=%d 值失败：%w", argsTerm, setTermErr)
				rf.logger.Trace(replyErr.Error())
				return
			}
		}
	}

	replyRes.Term = argsTerm
	replyRes.VoteGranted = false
	votedFor := rf.hardState.voted()
	if args.IsPreVote || votedFor == "" || votedFor == args.CandidateId {
		// 当前节点是追随者且没有投过票
		rf.logger.Trace("当前节点是追随者且没有投过票，开始比较日志的新旧程度")
		lastIndex := rf.lastEntryIndex()
		lastTerm := rf.lastEntryTerm()
		// 候选者的日志比当前节点的日志要新，则投票
		// 先比较 Term，Term 相同则比较日志长度
		if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
			rf.logger.Trace(fmt.Sprintf("候选者日志较新，args.lastTerm=%d, lastTerm=%d, args.lastIndex=%d, lastIndex=%d",
				args.LastLogTerm, lastTerm, args.LastLogIndex, lastIndex))
			voteErr := rf.hardState.vote(args.CandidateId)
			if voteErr != nil {
				replyErr = fmt.Errorf("更新 votedFor 出错，投票失败：%w", voteErr)
				rf.logger.Error(replyErr.Error())
				replyRes.VoteGranted = false
			} else {
				rf.logger.Trace("成功投出一张选票")
				replyRes.VoteGranted = true
			}
		} else {
			rf.logger.Trace(fmt.Sprintf("候选者日志不够新，不投票，args.lastTerm=%d, lastTerm=%d, args.lastIndex=%d, lastIndex=%d",
				args.LastLogTerm, lastTerm, args.LastLogIndex, lastIndex))
		}
	}

	if replyRes.VoteGranted {
		rf.timerState.setElectionTimer()
		rf.logger.Trace("设置选举计时器成功")
	}
}

// 慢 Follower 接收来自 Leader 的 InstallSnapshot 调用
// 目的是加快日志追赶速度
func (rf *raft) handleSnapshot(rpcMsg rpc) {

	// 重置选举计时器
	rf.timerState.setElectionTimer()
	rf.logger.Trace("重置选举计时器成功")

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
	if args.Term < rfTerm {
		// Leader 的 Term 过期，直接返回
		rf.logger.Trace("发送快照的 Leader 任期落后，直接返回")
		replyRes.Term = rfTerm
		return
	}

	// 任期数落后或相等，如果是候选者，需要降级
	// 后续操作都在 Follower / Learner 角色下完成
	stage := rf.roleState.getRoleStage()
	if args.Term > rfTerm && stage != Follower && stage != Learner {
		rf.logger.Trace("遇到更大的 Term 数，降级为 Follower")
		if !rf.becomeFollower(args.Term) {
			replyErr = fmt.Errorf("节点降级失败")
			return
		}
	}

	// 安装快照
	if installErr := rf.fsm.Install(args.Data); installErr != nil {
		replyErr = fmt.Errorf("安装快照失败：%w", installErr)
		return
	}
	rf.softState.setLastApplied(args.LastIncludedIndex)
	rf.logger.Trace("安装快照成功！")
	// 持久化快照
	replyRes.Term = rfTerm
	argsIndex := args.LastIncludedIndex
	snapshot := Snapshot{
		LastIndex: argsIndex,
		LastTerm:  args.LastIncludedTerm,
		Data:      args.Data,
	}
	if saveErr := rf.snapshotState.save(snapshot); saveErr != nil {
		replyErr = fmt.Errorf("持久化快照失败：%w", saveErr)
		return
	}
	rf.logger.Trace("持久化快照成功！")

	if !args.Done {
		// 若传送没有完成，则继续接收数据
		return
	}

	// 保存快照成功，删除多余日志
	lastIndex := rf.lastEntryIndex()
	if argsIndex < lastIndex {
		if !rf.entryExist(argsIndex) {
			replyErr = fmt.Errorf("收到的快照索引 %d 小于节点快照索引 %d", argsIndex, lastIndex)
		}
		entry, entryErr := rf.logEntry(argsIndex)
		if entryErr != nil {
			replyErr = fmt.Errorf("获取 index=%d 的日志失败！%w", argsIndex, entryErr)
			rf.logger.Error(replyErr.Error())
			return
		}
		if entry.Term == args.LastIncludedTerm {
			rf.logger.Trace("删除快照之前的旧日志")
			if truncateErr := rf.truncateBefore(argsIndex + 1); truncateErr != nil {
				replyErr = fmt.Errorf("删除日志失败！%w", truncateErr)
				rf.logger.Error(replyErr.Error())
			} else {
				rf.logger.Trace("删除日志成功！")
			}
		}
		return
	}

	lastEntryType := rf.lastEntryType()
	rf.logger.Trace("清空日志")
	rf.hardState.clearEntries()
	newEntry := Entry{
		Index: snapshot.LastIndex,
		Term:  snapshot.LastTerm,
		Type:  lastEntryType,
	}
	if appendEntryErr := rf.hardState.appendEntry(newEntry); appendEntryErr != nil {
		replyErr = fmt.Errorf("添加新日志失败！")
		rf.logger.Error(replyErr.Error())
	}
}

// 处理领导权转移请求
func (rf *raft) handleTransfer(rpcMsg rpc) {
	// 先发送一次心跳，刷新计时器，以及
	args := rpcMsg.req.(TransferLeadership)
	timer := time.After(rf.timerState.minElectionTimeout())
	// 设置定时器和rpc应答通道
	rf.leaderState.setTransferBusy(args.Transferee.Id)
	rf.leaderState.setTransferState(timer, rpcMsg.res)
	rf.logger.Trace("成功设置定时器和rpc应答通道")

	// 查看目标节点日志是否最新
	rf.logger.Trace("查看目标节点日志是否最新")
	rf.checkTransfer(args.Transferee.Id)
}

// 处理客户端请求
func (rf *raft) handleClientCmd(rpcMsg rpc) {

	// 重置心跳计时器
	if rf.isLeader() {
		rf.timerState.setHeartbeatTimer()
		rf.logger.Trace("重置心跳计时器成功")
	}

	args := rpcMsg.req.(ApplyCommand)
	var replyRes ApplyCommandReply
	var replyErr error
	defer func() {
		rpcMsg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
	}()

	// Leader 先将日志添加到内存
	rf.logger.Trace("将日志添加到内存")
	addEntryErr := rf.addEntry(Entry{Term: rf.hardState.currentTerm(), Type: EntryReplicate, Data: args.Data})
	if addEntryErr != nil {
		replyErr = fmt.Errorf("给 Leader 添加客户端日志失败：%w", addEntryErr)
		rf.logger.Trace(replyErr.Error())
		return
	}

	// 给各节点发送日志条目
	finishCh := make(chan finishMsg)
	stopCh := make(chan struct{})
	defer close(stopCh)
	rf.logger.Trace("给各节点发送日志条目")
	for id, addr := range rf.peerState.peers() {
		// 不用给自己发，正在复制日志的不发
		if rf.peerState.isMe(id) {
			rf.logger.Trace(fmt.Sprintf("自身节点，不发送心跳。Id=%s", id))
			rf.softState.setCommitIndex(rf.softState.getCommitIndex() + 1)
			go func() { finishCh <- finishMsg{msgType: Success, id: id} }()
			continue
		}
		if rf.leaderState.isRpcBusy(id) {
			rf.logger.Trace(fmt.Sprintf("忙节点，不发送心跳。Id=%s", id))
			go func() { finishCh <- finishMsg{msgType: Error} }()
		}
		// 发送日志
		go rf.replicationTo(id, addr, finishCh, stopCh, EntryReplicate)
	}

	// 新日志成功发送到过半 Follower 节点，提交本地的日志
	majorityFinishCh := make(chan bool)
	go func() {
		count := 0
		successCnt := 0
		sent := false
		after := time.After(rf.timerState.heartbeatDuration())
		for {
			select {
			case <-after:
				replyErr = fmt.Errorf("等待响应结果超时")
				rf.logger.Error(replyErr.Error())
				if !sent {
					majorityFinishCh <- false
					sent = true
				}
				return
			case msg := <-finishCh:
				if msg.msgType == Degrade {
					rf.logger.Trace("接收到降级请求")
					if rf.becomeFollower(msg.term) {
						rf.logger.Trace("降级成功")
					}
					replyErr = fmt.Errorf("节点降级")
					if !sent {
						majorityFinishCh <- false
						sent = true
					}
					return
				}
				if msg.msgType == Success {
					rf.logger.Trace(fmt.Sprintf("接收到 id=%s 的成功响应", msg.id))
					successCnt += 1
				}
				if successCnt >= rf.peerState.majority() {
					rf.logger.Trace("请求已成功发送给多数节点")
					if !sent {
						majorityFinishCh <- true
						sent = true
					}
					return
				}
				count += 1
				if count >= rf.peerState.peersCnt() {
					rf.logger.Trace("rpc 完成，所有节点都已返回响应")
					if !sent {
						replyErr = fmt.Errorf("日志未送达多数节点")
						majorityFinishCh <- false
						sent = true
					}
					return
				}
			}
		}
	}()

	success := <-majorityFinishCh
	if !success {
		replyErr = fmt.Errorf("日志发送未成功！")
		rf.logger.Error(replyErr.Error())
		return
	}

	// 将 commitIndex 设置为新条目的索引
	// 此操作会连带提交 Leader 先前未提交的日志条目并应用到状态季节
	rf.logger.Trace("Leader 更新 commitIndex")
	rf.updateLeaderCommit()
	rf.logger.Trace(fmt.Sprintf("commitIndex 日志更新为 %d", rf.softState.getCommitIndex()))

	// 应用状态机
	applyErr := rf.applyFsm()
	if applyErr != nil {
		replyErr = applyErr
		rf.logger.Error(replyErr.Error())
	}

	// 当日志量超过阈值时，生成快照
	rf.logger.Trace("检查是否需要生成快照")
	rf.updateSnapshot()

	replyRes.Status = OK
}

// 处理添加 Learner 节点请求
func (rf *raft) handleLearnerAdd(msg rpc) {
	learners := msg.req.(AddLearner).Learners
	replyRes := AddLearnerReply{}
	var replyErr error
	defer func() {
		msg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
	}()

	// 将新节点添加到 replication 集合
	for id, addr := range learners {
		if _, ok := rf.leaderState.replications[id]; !ok {
			// 开启复制循环
			rf.logger.Trace(fmt.Sprintf("开启复制循环。id=%s", id))
			replication := rf.newReplication(id, addr, Learner)
			rf.leaderState.replications[id] = replication
			go rf.addReplication(replication)
			go func() { replication.triggerCh <- struct{}{} }()
		}
	}
}

// 处理成员变更请求
func (rf *raft) handleConfigChange(msg rpc) {
	newConfig := msg.req.(ChangeConfig)
	replyRes := ChangeConfigReply{}
	var replyErr error
	defer func() {
		msg.res <- rpcReply{
			res: replyRes,
			err: replyErr,
		}
	}()

	// 先将所有 Learner 节点升级为 Follower
	promoteCh := make(chan finishMsg)
	promoteCnt := 0
	for id, addr := range newConfig.Peers {
		if rf.peerState.isMe(id) || rf.leaderState.getFollowerRole(id) != Learner {
			continue
		}
		promoteCnt += 1
		go func(id NodeId, addr NodeAddr) {
			finishCh := make(chan finishMsg)
			stopCh := make(chan struct{})
			defer func() {
				close(stopCh)
				close(finishCh)
			}()
			rf.logger.Trace("目标节点是 Learner 角色，发送 EntryPromote 请求")
			go rf.replicationTo(id, addr, finishCh, stopCh, EntryPromote)
			finish := <-finishCh
			if finish.msgType == Success {
				rf.leaderState.setReplicationRole(id, Follower)
				rf.logger.Trace("目标节点升级为 Follower 成功")
				promoteCh <- finishMsg{msgType: Success}
			} else {
				promoteCh <- finishMsg{msgType: Error}
			}
		}(id, addr)
	}

	for promoteCnt > 0 {
		timer := time.After(rf.timerState.heartbeatDuration())
		select {
		case <-timer:
			rf.logger.Trace("等待 Learner 升级超时")
			return
		case pmtMsg := <-promoteCh:
			if pmtMsg.msgType == Success {
				promoteCnt -= 1
			}
		}
	}

	// C(new) 配置
	newPeers := newConfig.Peers
	rf.leaderState.setNewConfig(newPeers)
	// C(old) 配置
	oldPeers := make(map[NodeId]NodeAddr)
	for id, addr := range rf.peerState.peers() {
		oldPeers[id] = addr
	}
	rf.leaderState.setOldConfig(oldPeers)
	rf.logger.Trace(fmt.Sprintf("旧配置：%+v，新配置%+v", oldPeers, newPeers))

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
	if oldNewConfigErr := rf.sendOldNewConfig(oldNewPeers); oldNewConfigErr != nil {
		replyErr = oldNewConfigErr
		rf.logger.Trace("C(old,new) 配置分发失败")
		return
	}

	// 分发 C(new) 配置
	rf.logger.Trace("分发 C(new) 配置")
	if newConfigErr := rf.sendNewConfig(newPeers); newConfigErr != nil {
		replyErr = newConfigErr
		rf.logger.Trace("C(new) 配置分发失败")
		return
	}

	// 清理 replications
	peers := rf.peerState.peers()
	// 如果当前节点被移除，退出程序
	if _, ok := peers[rf.peerState.myId()]; !ok {
		rf.logger.Trace("新配置中不包含当前节点，程序退出")
		go func() { rf.exitCh <- struct{}{} }()
		return
	}
	// 查看follower有没有被移除的
	rf.logger.Trace("删除新配置中不包含的 replication")
	followers := rf.leaderState.getReplications()
	for id, f := range followers {
		if _, ok := peers[id]; !ok {
			f.stopCh <- struct{}{}
			delete(followers, id)
		}
	}
	replyRes.Status = OK
}

func (rf *raft) updateSnapshot() {
	go func() {
		if rf.needGenSnapshot() {
			rf.logger.Trace("达成生成快照的条件")
			// 从状态机生成快照
			data, serializeErr := rf.fsm.Serialize()
			if serializeErr != nil {
				rf.logger.Error(fmt.Errorf("状态机生成快照失败！%w", serializeErr).Error())
			}
			rf.logger.Trace("状态机生成快照成功")
			// 持久化快照
			newSnapshot := Snapshot{
				LastIndex: rf.softState.getLastApplied(),
				LastTerm:  rf.hardState.currentTerm(),
				Data:      data,
			}
			saveErr := rf.snapshotState.save(newSnapshot)
			if saveErr != nil {
				rf.logger.Error(fmt.Errorf("保存快照失败！%w", serializeErr).Error())
			}
			rf.logger.Trace("持久化快照成功")
			// 清空日志
			lastEntryType := rf.lastEntryType()
			rf.logger.Trace("清空日志")
			rf.hardState.clearEntries()
			newEntry := Entry{
				Index: newSnapshot.LastIndex,
				Term:  newSnapshot.LastTerm,
				Type:  lastEntryType,
			}
			if appendEntryErr := rf.hardState.appendEntry(newEntry); appendEntryErr != nil {
				appendEntryErr = fmt.Errorf("添加新日志失败！")
				rf.logger.Error(appendEntryErr.Error())
			}
		}
	}()
}

func (rf *raft) checkTransfer(id NodeId) {
	select {
	case <-rf.leaderState.transfer.timer:
		rf.logger.Trace("领导权转移超时")
		rf.leaderState.setTransferBusy(None)
	default:
		if rf.leaderState.isRpcBusy(id) {
			// 若目标节点正在复制日志，则继续等待
			rf.logger.Trace("目标节点正在进行日志复制，继续等待")
			return
		}
		if rf.leaderState.matchIndex(id) == rf.lastEntryIndex() {
			// 目标节点日志已是最新，发送 timeoutNow 消息
			func() {
				var replyRes TransferLeadershipReply
				var replyErr error
				defer func() {
					rf.leaderState.transfer.reply <- rpcReply{
						res: replyRes,
						err: replyErr,
					}
				}()
				rf.logger.Trace(fmt.Sprintf("目标节点 Id=%s 日志已是最新，发送 timeoutNow 消息", id))
				finishCh := make(chan finishMsg)
				stopCh := make(chan struct{})
				defer func() {
					close(finishCh)
					close(stopCh)
				}()
				go rf.replicationTo(id, rf.peerState.peers()[id], finishCh, stopCh, EntryTimeoutNow)
				msg := <-finishCh
				if msg.msgType == Success {
					rf.becomeFollower(rf.hardState.currentTerm())
					rf.leaderState.setTransferBusy(None)
					replyRes.Status = OK
				} else {
					replyErr = fmt.Errorf("所有权转移失败：%d", msg.msgType)
				}
			}()
		} else {
			// 目标节点不是最新，开始日志复制
			rf.logger.Trace("目标节点不是最新，开始日志复制")
			rf.leaderState.replications[id].triggerCh <- struct{}{}
		}
	}
}

func (rf *raft) sendOldNewConfig(peers map[NodeId]NodeAddr) error {

	oldNewPeersData, enOldNewErr := encodePeersMap(peers)
	if enOldNewErr != nil {
		return fmt.Errorf("序列化peers字典失败！%w", enOldNewErr)
	}

	// C(old,new)配置添加到状态
	addEntryErr := rf.addEntry(Entry{Type: EntryChangeConf, Data: oldNewPeersData})
	if addEntryErr != nil {
		return fmt.Errorf("将配置添加到日志失败！%w", addEntryErr)
	}
	rf.peerState.replacePeers(peers)

	// C(old,new)发送到各个节点
	// 先给旧节点发，再给新节点发
	if rf.waitForConfig(rf.leaderState.getOldConfig()) {
		rf.logger.Trace("配置成功发送到旧节点的多数")
		if rf.waitForConfig(rf.leaderState.getNewConfig()) {
			rf.logger.Trace("配置成功发送到新节点的多数")
			return nil
		} else {
			rf.logger.Trace("配置复制到新配置多数节点失败")
			return fmt.Errorf("配置未复制到新配置多数节点")
		}
	} else {
		rf.logger.Trace("配置复制到旧配置多数节点失败")
		return fmt.Errorf("配置未复制到旧配置多数节点")
	}
}

func (rf *raft) sendNewConfig(peers map[NodeId]NodeAddr) error {

	// C(old,new)配置
	oldNewPeers := rf.peerState.peers()

	newPeersData, enOldNewErr := encodePeersMap(peers)
	if enOldNewErr != nil {
		return fmt.Errorf("新配置序列化失败！%w", enOldNewErr)
	}

	// C(new)配置添加到状态
	addEntryErr := rf.addEntry(Entry{Type: EntryChangeConf, Data: newPeersData})
	if addEntryErr != nil {
		return fmt.Errorf("将配置添加到日志失败！%w", addEntryErr)
	}
	rf.peerState.replacePeers(peers)
	rf.logger.Trace("替换掉当前节点的 Peers 配置")

	// C(new)配置发送到各个节点
	finishCh := make(chan finishMsg)
	stopCh := make(chan struct{})
	defer close(stopCh)
	rf.logger.Trace("给各节点发送新配置")
	for id, addr := range oldNewPeers {
		// 不用给自己发
		if rf.peerState.isMe(id) {
			continue
		}
		// 发送日志
		rf.logger.Trace(fmt.Sprintf("给 Id=%s 的节点发送配置", id))
		go rf.replicationTo(id, addr, finishCh, stopCh, EntryChangeConf)
	}

	count := 1
	successCnt := 1
	end := false
	after := time.After(rf.timerState.heartbeatDuration())
	for !end {
		select {
		case <-after:
			return fmt.Errorf("请求超时")
		case msg := <-finishCh:
			if msg.msgType == Degrade {
				rf.logger.Trace("接收到降级请求")
				if rf.becomeFollower(msg.term) {
					rf.logger.Trace("降级成功")
					return fmt.Errorf("降级为 Follower")
				}
			}
			if msg.msgType == Success {
				successCnt += 1
			}
			count += 1
			if successCnt >= rf.peerState.majority() {
				rf.logger.Trace("已发送到大多数节点")
				end = true
				break
			}
			if count >= rf.peerState.peersCnt() {
				return fmt.Errorf("各节点已响应，但成功数不占多数")
			}
		}
	}

	// 提交日志
	rf.logger.Trace("提交新配置日志")
	rf.softState.setCommitIndex(rf.lastEntryIndex())
	return nil
}

func (rf *raft) waitForConfig(peers map[NodeId]NodeAddr) bool {
	finishCh := make(chan finishMsg)
	stopCh := make(chan struct{})
	defer close(stopCh)

	for id, addr := range peers {
		// 不用给自己发
		if rf.peerState.isMe(id) {
			continue
		}
		// 发送日志
		rf.logger.Trace(fmt.Sprintf("给节点 Id=%s 发送最新条目", id))
		go rf.replicationTo(id, addr, finishCh, stopCh, EntryChangeConf)
	}

	count := 1
	successCnt := 1
	end := false
	after := time.After(rf.timerState.heartbeatDuration())
	for !end {
		select {
		case <-after:
			end = true
			rf.logger.Trace("超时退出")
		case result := <-finishCh:
			if result.msgType == Degrade {
				rf.logger.Trace("接收到降级消息")
				if rf.becomeFollower(result.term) {
					rf.logger.Trace("降级为 Follower")
					return false
				}
				rf.logger.Trace("降级失败")
			}
			if result.msgType == Success {
				rf.logger.Trace("接收到一个成功响应")
				successCnt += 1
			}
			count += 1
			if successCnt >= rf.peerState.majority() {
				rf.logger.Trace("多数节点已成功响应")
				end = true
				break
			}
			if count >= rf.peerState.peersCnt() {
				rf.logger.Trace("接收到所有响应，但成功不占多数")
				return false
			}
		}
	}

	// 提交日志
	rf.logger.Trace("提交日志")
	oldNewIndex := rf.lastEntryIndex()
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
func (rf *raft) replicationTo(id NodeId, addr NodeAddr, finishCh chan finishMsg, stopCh chan struct{}, entryType EntryType) {
	var msg finishMsg
	defer func() {
		select {
		case <-stopCh:
		default:
			msg.id = id
			finishCh <- msg
		}
	}()

	// 检查是否需要发送快照
	rf.logger.Trace("检查是否需要发送快照")
	if !rf.checkSnapshot(rf.leaderState.replications[id]) {
		rf.logger.Error("发送快照失败！")
		msg = finishMsg{msgType: RpcFailed}
		return
	}

	rf.logger.Trace(fmt.Sprintf("给节点 %s 发送 %s 类型的 entry", id, EntryTypeToString(entryType)))

	// 发起 RPC 调用
	prevIndex := rf.leaderState.nextIndex(id) - 1
	// 获取最新的日志
	var entries []Entry
	if entryType != EntryHeartbeat && entryType != EntryPromote && entryType != EntryTimeoutNow {
		lastEntryIndex := rf.lastEntryIndex()
		entry, err := rf.logEntry(lastEntryIndex)
		if err != nil {
			msg = finishMsg{msgType: Error}
			rf.logger.Error(fmt.Errorf("获取 index=%d 日志失败 %w", lastEntryIndex, err).Error())
			return
		}
		entries = []Entry{entry}
	}
	var prevTerm int
	// 获取 prev 日志
	prevEntry, prevEntryErr := rf.logEntry(prevIndex)
	if prevEntryErr != nil {
		msg = finishMsg{msgType: Error}
		rf.logger.Error(fmt.Errorf("获取 index=%d 日志失败 %w", prevIndex, prevEntryErr).Error())
		return
	}
	prevTerm = prevEntry.Term

	args := AppendEntry{
		EntryType:    entryType,
		Term:         rf.hardState.currentTerm(),
		LeaderId:     rf.peerState.myId(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.softState.getCommitIndex(),
	}
	res := &AppendEntryReply{}
	rf.logger.Trace(fmt.Sprintf("发送的内容：%+v", args))
	rpcErr := rf.transport.AppendEntries(addr, args, res)

	// 处理 RPC 调用结果
	if rpcErr != nil {
		rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, rpcErr).Error())
		msg = finishMsg{msgType: RpcFailed}
		return
	}

	if res.Term > rf.hardState.currentTerm() {
		// 当前任期数落后，降级为 Follower
		rf.logger.Trace("任期落后，发送降级通知")
		msg = finishMsg{msgType: Degrade, term: res.Term}
		return
	}

	if res.Success {
		msg = finishMsg{msgType: Success, id: id}
		if entryType == EntryReplicate {
			rf.leaderState.matchAndNextIndexAdd(id)
		}
		return
	}

	checkEntryType := entryType == EntryReplicate || entryType == EntryHeartbeat
	checkProgress := rf.softState.getCommitIndex() > rf.leaderState.matchIndex(id)
	if checkEntryType && checkProgress && !rf.leaderState.isRpcBusy(id) {
		rf.logger.Trace(fmt.Sprintf("节点 id=%s 日志落后，开始 FindNextIndex 追赶", id))
		rf.leaderState.replications[id].triggerCh <- struct{}{}
		rf.logger.Trace("已触发 FindNextIndex 追赶")
	}
}

// 日志追赶
func (rf *raft) replicate(s *Replication) bool {

	// 如果缺失的日志太多时，直接发送快照
	rf.logger.Trace("检查是否需要发送快照")
	if !rf.checkSnapshot(s) {
		rf.logger.Trace("日志追赶失败")
		return false
	}

	// 向前查找 nextIndex 值
	rf.logger.Trace("向前查找 nextIndex 值")
	if !rf.findCorrectNextIndex(s) {
		rf.logger.Trace("日志追赶失败")
		return false
	}

	// 递增更新 matchIndex 值
	rf.logger.Trace("递增更新 matchIndex 值")
	return rf.findCorrectMatchIndex(s)

}

func (rf *raft) checkSnapshot(s *Replication) bool {
	snapshot := rf.snapshotState.getSnapshot()
	finishCh := make(chan finishMsg)
	if rf.leaderState.nextIndex(s.id) <= snapshot.LastIndex {
		rf.logger.Trace(fmt.Sprintf("节点 Id=%s 缺失的日志太多，直接发送快照", s.id))
		go rf.snapshotTo(s.addr, finishCh, make(chan struct{}))
		msg := <-finishCh
		if msg.msgType != Success {
			if msg.msgType == RpcFailed {
				rf.logger.Error(fmt.Sprintf("对 id=%s 节点的 rpc 调用失败", s.id))
				return false
			}
			if msg.msgType == Degrade {
				rf.logger.Trace("接收到降级通知")
				if rf.becomeFollower(msg.term) {
					rf.logger.Trace("降级为 Follower 成功！")
				}
				return false
			}
		}
		rf.logger.Trace("快照发送成功！")
		rf.leaderState.setMatchAndNextIndex(s.id, snapshot.LastIndex, snapshot.LastIndex+1)
		if snapshot.LastIndex == rf.lastEntryIndex() {
			rf.logger.Trace("快照后面没有新日志，日志追赶结束")
			return true
		}
	}
	return true
}

func (rf *raft) findCorrectNextIndex(s *Replication) bool {
	rl := rf.leaderState

	for rl.nextIndex(s.id) > 0 {
		select {
		case <-s.stopCh:
			return false
		default:
		}
		nextIndex := rl.nextIndex(s.id)
		prevIndex := nextIndex - 1
		prevEntry, prevEntryErr := rf.logEntry(prevIndex)
		if prevEntryErr != nil {
			rf.logger.Error(fmt.Errorf("获取 index=%d 日志失败 %w", prevIndex, prevEntryErr).Error())
			return false
		}
		args := AppendEntry{
			EntryType:    EntryHeartbeat,
			Term:         rf.hardState.currentTerm(),
			LeaderId:     rf.peerState.myId(),
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevEntry.Term,
			LeaderCommit: rf.softState.getCommitIndex(),
			Entries:      []Entry{},
		}
		res := &AppendEntryReply{}
		rf.logger.Trace(fmt.Sprintf("给节点 Id=%s 发送日志：%+v", s.id, args))
		err := rf.transport.AppendEntries(s.addr, args, res)

		if err != nil {
			rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", s.addr, err).Error())
			return false
		}
		rf.logger.Trace(fmt.Sprintf("接收到节点 id=%s 的应答 %+v", s.id, res))
		// 如果任期数小，降级为 Follower
		if res.Term > rf.hardState.currentTerm() {
			rf.logger.Trace("当前任期数小，降级为 Follower")
			if rf.becomeFollower(res.Term) {
				rf.logger.Trace("降级成功")
			}
			return false
		}
		if res.Success {
			rf.logger.Trace("日志匹配成功！")
			return true
		}

		conflictStartIndex := res.ConflictStartIndex
		// Follower 日志是空的，则 nextIndex 置为 1
		if conflictStartIndex <= 0 {
			conflictStartIndex = 1
		}
		// conflictStartIndex 处的日志是一致的，则 nextIndex 置为下一个
		if entry, entryErr := rf.logEntry(conflictStartIndex); entryErr != nil {
			rf.logger.Error(fmt.Errorf("获取 index=%d 日志失败 %w", conflictStartIndex, entryErr).Error())
			return false
		} else if entry.Term == res.ConflictTerm {
			conflictStartIndex += 1
		}

		// 向前继续查找 Follower 缺少的第一条日志的索引
		rf.logger.Trace(fmt.Sprintf("设置节点 Id=%s 的 nextIndex 为 %d", s.id, conflictStartIndex))
		rl.setNextIndex(s.id, conflictStartIndex)
	}
	return true
}

func (rf *raft) findCorrectMatchIndex(s *Replication) bool {

	rl := rf.leaderState
	// 发送单个日志
	for rl.nextIndex(s.id)-1 < rf.lastEntryIndex() {
		select {
		case <-s.stopCh:
			return false
		default:
		}

		nextIndex := rl.nextIndex(s.id)
		prevIndex := nextIndex - 1
		prevEntry, prevErr := rf.logEntry(prevIndex)
		if prevErr != nil {
			rf.logger.Error(fmt.Errorf("获取 index=%d 日志失败 %w", prevIndex, prevErr).Error())
			return false
		}
		var entries []Entry
		sendEntry, sendEntryErr := rf.logEntry(nextIndex)
		if sendEntryErr != nil {
			rf.logger.Error(fmt.Errorf("获取 index=%d 日志失败 %w", nextIndex, sendEntryErr).Error())
			return false
		} else {
			entries = []Entry{sendEntry}
		}
		args := AppendEntry{
			Term:         rf.hardState.currentTerm(),
			LeaderId:     rf.peerState.myId(),
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevEntry.Term,
			LeaderCommit: rf.softState.getCommitIndex(),
			Entries:      entries,
		}
		res := &AppendEntryReply{}
		rf.logger.Trace(fmt.Sprintf("给 Id=%s 发送日志 %+v", s.id, args))
		rpcErr := rf.transport.AppendEntries(s.addr, args, res)

		if rpcErr != nil {
			rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", s.addr, rpcErr).Error())
			return false
		}
		if res.Term > rf.hardState.currentTerm() {
			rf.logger.Trace("任期数小，开始降级")
			if rf.becomeFollower(res.Term) {
				rf.logger.Trace("降级为 Follower 成功！")
			}
			return false
		}

		// 向后补充
		matchIndex := rl.nextIndex(s.id)
		rf.logger.Trace(fmt.Sprintf("设置节点 Id=%s 的状态：matchIndex=%d, nextIndex=%d", s.id, matchIndex, matchIndex+1))
		rf.leaderState.setMatchAndNextIndex(s.id, matchIndex, matchIndex+1)
	}
	return true
}

func (rf *raft) snapshotTo(addr NodeAddr, finishCh chan finishMsg, stopCh chan struct{}) {
	var msg finishMsg
	defer func() {
		select {
		case <-stopCh:
		default:
			finishCh <- msg
		}
	}()
	snapshot := rf.snapshotState.getSnapshot()
	args := InstallSnapshot{
		Term:              rf.hardState.currentTerm(),
		LeaderId:          rf.peerState.myId(),
		LastIncludedIndex: snapshot.LastIndex,
		LastIncludedTerm:  snapshot.LastTerm,
		Offset:            0,
		Data:              snapshot.Data,
		Done:              true,
	}
	var res InstallSnapshotReply
	rf.logger.Trace(fmt.Sprintf("向节点 %s 发送快照：%+v", addr, args))
	err := rf.transport.InstallSnapshot(addr, args, &res)
	if err != nil {
		rf.logger.Error(fmt.Errorf("调用rpc服务失败：%s%w\n", addr, err).Error())
		msg = finishMsg{msgType: RpcFailed}
		return
	}
	if res.Term > rf.hardState.currentTerm() {
		// 如果任期数小，降级为 Follower
		rf.logger.Trace("任期数小，发送降级通知")
		msg = finishMsg{msgType: Degrade, term: res.Term}
		return
	}
	rf.logger.Trace(fmt.Sprintf("快照在节点 %s 安装完毕", addr))
	msg = finishMsg{msgType: Success}
}

// 当前节点是不是 Leader
func (rf *raft) isLeader() bool {
	roleStage := rf.roleState.getRoleStage()
	leaderIsMe := rf.peerState.leaderIsMe()
	return roleStage == Leader && leaderIsMe
}

func (rf *raft) becomeLeader() bool {
	rf.setRoleStage(Leader)
	rf.peerState.setLeader(rf.peerState.myId())

	// 给各个节点发送心跳，建立权柄
	finishCh := make(chan finishMsg)
	stopCh := make(chan struct{})
	rf.logger.Trace("给各个节点发送心跳，建立权柄")
	for id, addr := range rf.peerState.peers() {
		if rf.peerState.isMe(id) {
			continue
		}
		rf.logger.Trace(fmt.Sprintf("给 Id=%s 发送心跳", id))
		go rf.replicationTo(id, addr, finishCh, stopCh, EntryHeartbeat)
	}
	rf.onRoleChange(Leader)
	return true
}

func (rf *raft) becomeCandidate() bool {
	// 角色置为候选者
	rf.setRoleStage(Candidate)
	rf.onRoleChange(Candidate)
	return true
}

// 降级为 Follower
func (rf *raft) becomeFollower(term int) bool {
	rf.logger.Trace("设置节点 Term 值")
	err := rf.hardState.setTerm(term)
	if err != nil {
		rf.logger.Error(fmt.Errorf("term 值设置失败，降级失败%w", err).Error())
		return false
	}
	rf.setRoleStage(Follower)
	rf.onRoleChange(Follower)
	return true
}

func (rf *raft) setRoleStage(stage RoleStage) {
	rf.roleState.setRoleStage(stage)
	rf.logger.Trace(fmt.Sprintf("角色设置为 %s", RoleToString(stage)))
	if stage == Leader {
		rf.peerState.setLeader(rf.peerState.myId())
	}
}

// 添加新日志
func (rf *raft) addEntry(entry Entry) error {
	entry.Index = rf.lastEntryIndex() + 1
	rf.logger.Trace(fmt.Sprintf("日志条目索引 index=%d", entry.Index))
	return rf.hardState.appendEntry(entry)
}

// 把日志应用到状态机
func (rf *raft) applyFsm() (err error) {
	commitIndex := rf.softState.getCommitIndex()
	lastApplied := rf.softState.getLastApplied()

	for commitIndex > lastApplied {
		if entry, entryErr := rf.logEntry(lastApplied + 1); entryErr != nil {
			err = fmt.Errorf("获取 index=%d 日志失败 %w", lastApplied+1, entryErr)
			rf.logger.Error(err.Error())
			return
		} else {
			applyErr := rf.fsm.Apply(entry.Data)
			if applyErr != nil {
				if err == nil {
					err = fmt.Errorf("应用状态机失败，%w", applyErr)
				} else {
					err = fmt.Errorf("%w", err)
				}
			}
			lastApplied = rf.softState.lastAppliedAdd()
		}
	}

	return
}

// 更新 Leader 的提交索引
func (rf *raft) updateLeaderCommit() {
	commitIndexes := make([]int, 0)
	for id := range rf.peerState.peers() {
		if rf.peerState.isMe(id) {
			commitIndexes = append(commitIndexes, rf.softState.getCommitIndex())
		} else {
			commitIndexes = append(commitIndexes, rf.leaderState.matchIndex(id))
		}
	}
	sort.Ints(commitIndexes)
	rf.softState.setCommitIndex(commitIndexes[rf.peerState.majority()-1])
}

func (rf *raft) needGenSnapshot() bool {
	archiveThreshold := rf.softState.getCommitIndex()-rf.snapshotState.lastIndex() >= rf.snapshotState.logThreshold()
	return archiveThreshold && rf.lastEntryType() != EntryChangeConf
}

func (rf *raft) lastEntry() Entry {
	snapshot := rf.snapshotState.getSnapshot()
	if snapshot == nil {
		log.Fatalln("快照不存在！")
	}
	entry, _ := rf.hardState.logEntry(rf.hardState.logLength() - 1)
	return entry
}

func (rf *raft) lastEntryIndex() int {
	snapshot := rf.snapshotState.getSnapshot()
	if snapshot == nil {
		log.Fatalln("快照不存在！")
	}
	entry, _ := rf.hardState.logEntry(rf.hardState.logLength() - 1)
	return entry.Index
}

func (rf *raft) lastEntryTerm() int {
	snapshot := rf.snapshotState.getSnapshot()
	if snapshot == nil {
		log.Fatalln("快照不存在！")
	}
	entry, _ := rf.hardState.logEntry(rf.hardState.logLength() - 1)
	return entry.Term
}

func (rf *raft) lastEntryType() (entryType EntryType) {
	snapshot := rf.snapshotState.getSnapshot()
	if snapshot == nil {
		log.Fatalln("快照不存在！")
	}
	entry, _ := rf.hardState.logEntry(rf.hardState.logLength() - 1)
	return entry.Type
}

func (rf *raft) entryExist(index int) bool {
	snapshot := rf.snapshotState.getSnapshot()
	if snapshot == nil {
		log.Fatalln("快照不存在！")
	}
	return index > snapshot.LastIndex
}

func (rf *raft) logEntry(index int) (entry Entry, err error) {
	snapshot := rf.snapshotState.getSnapshot()
	if snapshot == nil {
		log.Fatalln("快照不存在！")
	}
	if index < snapshot.LastIndex {
		err = errors.New(fmt.Sprintf("索引 %d 小于等于快照索引 %d，不合法操作", index, snapshot.LastIndex))
	} else {
		if iEntry, iEntryErr := rf.hardState.logEntry(index - snapshot.LastIndex); iEntryErr != nil {
			err = fmt.Errorf(iEntryErr.Error())
		} else {
			entry = iEntry
		}
	}
	return
}

// 将当前索引及之后的日志删除
func (rf *raft) truncateAfter(index int) (err error) {
	if snapshot := rf.snapshotState.getSnapshot(); snapshot != nil {
		if index <= snapshot.LastIndex {
			err = errors.New(fmt.Sprintf("索引 %d 小于快照索引 %d，不合法操作", index, snapshot.LastIndex))
		} else {
			rf.hardState.truncateAfter(index - snapshot.LastIndex)
		}
	} else {
		rf.hardState.truncateAfter(index)
	}
	return
}

// 将当前索引之前的日志删除
// 实际上保留了最后一个日志，此日志的 Index 和快照的 LastIndex 相同
func (rf *raft) truncateBefore(index int) (err error) {
	if snapshot := rf.snapshotState.getSnapshot(); snapshot != nil {
		if index <= snapshot.LastIndex {
			err = errors.New(fmt.Sprintf("索引 %d 小于快照索引 %d，不合法操作", index, snapshot.LastIndex))
		} else {
			rf.hardState.truncateBefore(index - snapshot.LastIndex)
		}
	} else {
		rf.hardState.truncateBefore(index)
	}
	return
}

func (rf *raft) addRoleObserver(ob chan RoleStage) {
	rf.obMu.Lock()
	rf.obMu.Unlock()
	rf.roleObserver = append(rf.roleObserver, ob)
}

func (rf *raft) onRoleChange(role RoleStage) {
	if len(rf.roleObserver) <= 0 {
		return
	}
	for _, ob := range rf.roleObserver {
		go func(ob chan RoleStage) {
			ob <- role
		}(ob)
	}
}
