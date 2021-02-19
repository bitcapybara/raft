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

func (rf *raft) commitIndex() int {
	return rf.softState.softCommitIndex()
}

func (rf *raft) lastLogIndex() int {
	return rf.hardState.lastLogIndex()
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

func (rf *raft) logEntryTerm(index int) int {
	return rf.hardState.logEntryTerm(index)
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

// 降级为 Follower
func (rf *raft) degrade(term int) error {
	if rf.roleState.getRoleStage() == Follower {
		return nil
	}
	// 更新状态
	rf.mu.Lock()
	rf.mu.Unlock()
	rf.roleState.setRoleStage(Follower)
	return rf.hardState.setTerm(term)
}

// 更新自身 term
func (rf *raft) setTerm(term int) error {
	if rf.term() == term {
		return nil
	}
	// 更新状态
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.hardState.setTerm(term)
}

// 发送心跳
func (rf *raft) heartbeat() {
	commitIndex := rf.commitIndex()
	if !rf.isLeader() || commitIndex < rf.lastLogIndex() {
		return
	}
	var wg sync.WaitGroup
	for id, addr := range rf.peers() {
		wg.Add(1)
		go func(id NodeId, addr NodeAddr) {
			defer wg.Done()
			if rf.isMe(id) {
				return
			}
			if rf.matchIndex(id) < rf.lastLogIndex() {
				// 没有跟上进度的 Follower 正在进行日志复制，不发送心跳
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
				err := rf.degrade(res.term)
				if err != nil {
					log.Println(err)
				}
			}
		}(id, addr)
	}
	wg.Wait()
}

// 开启新一轮选举
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
	if int(vote) > rf.majority() {
		// 获得了大多数选票，转换为 Leader
		rf.setRoleStage(Leader)
		rf.setLeader(rf.me())
		for id := range rf.peers() {
			rf.setMatchAndNextIndex(id, 0, rf.lastLogIndex() + 1)
		}
	}
}
