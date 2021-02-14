package core

import (
	"errors"
	"github.com/bitcapybara/go-raft/util"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type roleType int

// 节点的角色类型
const (
	Leader roleType = iota
	Candidate
	Follower
)

type AppendEntry struct {
	term         int     // 当前时刻所属任期
	leaderId     nodeId  // 领导者的地址，方便 follower 重定向
	prevLogIndex int     // 要发送的日志条目的前一个条目的索引
	prevLogTerm  int     // prevLogIndex 条目所处任期
	leaderCommit int     // Leader 提交的索引
	entries      []entry // 日志条目（心跳为空；为提高效率可能发送多个）
}

type AppendEntryReply struct {
	term    int  // 当前时刻所属任期，用于领导者更新自身
	success bool // 如果关注者包含与prevLogIndex和prevLogTerm匹配的条目，则为true
}

type RequestVote struct {
	term          int    // 当前时刻所属任期
	candidateAddr string // 候选人地址
	lastLogIndex  int    // 发送此请求的 Candidate 最后一个日志条目的索引
	lastLogTerm   int    // lastLogIndex 所处的任期
}

type RequestVoteReply struct {
	term        int  // 当前时刻所属任期，用于领导者更新自身
	voteGranted bool // 为 true 表示候选人收到一个选票
}

type nodeId string

type nodeAddr string

type raft struct {
	// 当前节点的角色
	roleType roleType

	// 当前时刻所处的 term
	term int

	// 客户端状态机
	fsm Fsm

	// 所有节点
	peers map[nodeId]nodeAddr

	// 当前节点在 peers 中的索引
	me nodeId

	// 当前 leader 在 peers 中的索引
	leader nodeId

	// 当前节点保存的日志
	entries entries

	// 已经提交的最大的日志索引，由当前节点维护。
	commitIndex int

	// 应用到状态机的最后一个日志索引
	lastApplied int

	// 下一次要发送给各节点的日志索引。由 Leader 维护，初始值为 Leader 最后一个日志的索引 + 1
	nextIndex map[nodeId]int

	// 已经复制到各节点的最大的日志索引。由 Leader 维护，初始值为0
	matchIndex map[nodeId]int

	// 当前节点为 Follower / Candidate 时，为选举超时计时器
	// 当前节点为 Leader 时，为心跳计时器
	timer *time.Timer

	// 选举投票记录，仅在当前角色为 Follower 时可用
	voteMap map[int]bool

	mu sync.Mutex
}

func NewRaft(peers map[nodeId]nodeAddr, me nodeId, fsm Fsm) *raft {
	rf := new(raft)
	rf.roleType = Follower
	rf.term = 0
	rf.fsm = fsm
	rf.peers = peers
	rf.me = me
	rf.leader = ""
	rf.entries = make(entries, 0)
	rf.commitIndex = 0
	for id := range peers {
		rf.matchIndex[id] = 0
		rf.nextIndex[id] = 1
	}
	rf.voteMap = make(map[int]bool)
	return rf
}

func (r *raft) Start() {
	// 设定计时器，开始后台线程
	go r.loop()

	// 开放 rpc 服务接口
	rpcServer := rpc.NewServer()
	err := rpcServer.Register(r)
	if err != nil {
		log.Fatalf("开启 rpc 服务失败！%s", err)
	}
	// 开启监听器
	listener, err := net.Listen("tcp", string(r.peers[r.me]))
	if err != nil {
		log.Fatalf("开启服务监听器失败！%s", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Fatalf("开启服务监听器失败！%s\n", err)
		}
	}()

	// 接收请求并处理
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("accept error", err)
			break
		} else {
			go rpcServer.ServeConn(conn)
		}
	}
}

// 后台循环执行的定时器
func (r *raft) loop() {
	for {
		if r.roleType == Leader {
			r.leaderLoop()
		} else {
			r.normalLoop()
		}
	}
}

// leader 循环
func (r *raft) leaderLoop() {
	r.setTimer(30, 50)
	for range r.timer.C {
		// 每隔一段时间发送心跳
		r.mu.Lock()
		for id, addr := range r.peers {
			if id == r.me {
				continue
			}
			client, err := rpc.Dial("tcp", string(addr))
			if err != nil {
				log.Printf("创建rpc客户端失败：%s%s\n", addr, err)
				continue
			}
			args := AppendEntry{
				term:         r.term,
				leaderId:     r.me,
				prevLogIndex: r.commitIndex,
				prevLogTerm:  r.entries[r.commitIndex].term,
				entries:      nil,
				leaderCommit: r.commitIndex,
			}
			res := &AppendEntryReply{}
			err = client.Call("raft.requestVote", args, res)
			if err != nil {
				log.Printf("调用rpc服务失败：%s%s\n", addr, err)
			}
			err = client.Close()
			if err != nil {
				log.Println(err)
			}
			if res.term > r.term {
				// 当前任期数落后，降级为 Follower
				r.roleType = Follower
				r.term = res.term
				r.voteMap = make(map[int]bool)
				break
			}
		}

		r.mu.Lock()
	}
}

// follower 和 candidate 循环
func (r *raft) normalLoop() {
	r.setTimer(300, 500)
	for range r.timer.C {
		// 等待超时，开始新一轮竞选
		r.mu.Lock()
		// 转换为 candidate
		r.roleType = Candidate
		// 发送投票请求
		vote := 0
		r.term += 1
		for id, addr := range r.peers {
			if id == r.me {
				// 投票给自己
				vote += 1
				continue
			}
			client, err := rpc.Dial("tcp", string(addr))
			if err != nil {
				log.Printf("创建rpc客户端失败：%s%s\n", addr, err)
				continue
			}

			args := RequestVote{
				term:          r.term,
				candidateAddr: string(addr),
			}
			res := &RequestVoteReply{}
			err = client.Call("raft.requestVote", args, res)
			if err != nil {
				log.Printf("调用rpc服务失败：%s%s\n", addr, err)
			}
			err = client.Close()
			if err != nil {
				log.Println(err)
			}
			if res.term <= r.term && res.voteGranted {
				// 成功获得选票
				vote += 1
			} else if res.term > r.term {
				// 当前节点任期落后，则退出竞选
				r.roleType = Follower
				r.term = res.term
				break
			}
		}
		if vote >= len(r.peers)/2 {
			// 获得了大多数选票，转换为 Leader
			r.roleType = Leader
			r.leader = r.me
			for id := range r.peers {
				r.matchIndex[id] = 0
				r.nextIndex[id] = len(r.entries) + 1
			}
		}
		r.mu.Unlock()
	}
}

func (r *raft) setTimer(min int, max int) {
	r.mu.Lock()
	if r.timer == nil {
		r.timer = time.NewTimer(time.Millisecond * time.Duration(util.RandInt(min, max)))
	} else {
		r.timer.Reset(time.Millisecond * time.Duration(util.RandInt(min, max)))
	}
	r.mu.Unlock()
}

// follower 和 candidate 开放的 rpc接口，由 leader 调用
func (r *raft) appendEntries(args AppendEntry, res *AppendEntryReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// ========== 接收日志条目 ==========
	if len(args.entries) != 0 {
		// todo 日志复制
		return nil
	}

	// ========== 接收心跳 ==========
	if args.term >= r.term {
		// 任期数落后或相等
		if r.roleType == Candidate {
			// 如果是候选者，需要降级
			r.roleType = Follower
			r.voteMap = make(map[int]bool)
		}
		r.term = args.term
		r.leader = args.leaderId
		res.term = r.term
		res.success = true
		r.setTimer(300, 500)
		return nil
	}

	// 发送心跳的 Leader 任期数落后
	res.term = r.term
	res.success = true
	return nil
}

// follower 和 candidate 开放的 rpc接口，由 candidate 调用
func (r *raft) requestVote(args RequestVote, res *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	argsTerm := args.term

	// 拉票的候选者任期落后
	if argsTerm < r.term {
		res.term = r.term
		res.voteGranted = false
		return nil
	}

	// 当前节点所处任期数落后
	if argsTerm > r.term {
		if r.roleType == Candidate {
			// 当前节点是候选者，自动降级，投出第一票
			r.roleType = Follower
			res.term = argsTerm
			r.voteMap = make(map[int]bool)
			r.voteMap[argsTerm] = true
			res.voteGranted = true
		} else if !r.voteMap[argsTerm] {
			// 当前节点是追随者且没有投过票，则投出第一票
			res.voteGranted = true
		} else {
			// 当前节点是追随者且已投过票，则不投票
			res.voteGranted = false
		}
		res.term = argsTerm
		r.term = argsTerm
		return nil
	}

	// 拉票者和当前节点任期数相同
	// 无论当前节点是候选者还是追随者，都不投票
	if argsTerm == r.term {
		res.term = argsTerm
		res.voteGranted = false
	}
	return nil
}

type ClientRequest struct {
	data []byte // 客户端请求应用到状态机的数据
}

type ClientResponse struct {
	data []byte // 状态机应用数据后返回的结果
}

// 当前节点是 Leader 时开放的 rpc 接口，接受客户端请求
func (r *raft) Request(request ClientRequest, response *ClientResponse) error {

	// 当前节点是 Leader 才接受此调用
	isLeader := r.me == r.leader
	if !isLeader {
		return errors.New("当前节点不是 Leader")
	}

	r.mu.Lock()
	// 日志复制和心跳互斥
	r.timer.Stop()
	defer func() {
		r.mu.Unlock()
		r.setTimer(30, 50)
	}()

	// 日志复制
	newEntry := entry{
		index: r.commitIndex + 1,
		term: r.term,
		data: request.data,
	}
	finalPrevIndex := len(r.entries) - 1
	r.entries = append(r.entries, newEntry)

	successCnt := 0
	for id, addr := range r.peers {
		// 遍历所有 Follower，发送日志
		if id == r.me {
			successCnt += 1
			continue
		}
		client, err := rpc.Dial("tcp", string(addr))
		if err != nil {
			log.Printf("建立连接失败！%s\n", err)
			continue
		}
		var prevIndex int
		for {
			prevIndex = r.nextIndex[id] - 1
			// Follower 节点中必须存在第 prevIndex 条日志，才能接受第 nextIndex 条日志
			// 如果 Follower 节点缺少很多日志，需要找到缺少的第一条日志的索引
			args := AppendEntry{
				term: r.term,
				leaderId: r.me,
				prevLogIndex: prevIndex,
				prevLogTerm: r.entries[prevIndex].term,
				leaderCommit: r.commitIndex,
				entries: r.entries[prevIndex:r.nextIndex[id]],
			}
			res := &AppendEntryReply{}
			err = client.Call("raft.AppendEntries", args, res)
			if err != nil {
				log.Println(err)
			}
			if res.term > r.term {
				// 如果任期数小，降级为 Follower
				r.roleType = Follower
				r.voteMap = make(map[int]bool)
				err = client.Close()
				if err != nil {
					log.Println(err)
				}
				break
			}
			if res.success {
				break
			}

			// 向前继续查找 Follower 缺少的第一条日志的索引
			r.nextIndex[id] = r.nextIndex[id] - 1
		}

		for prevIndex != finalPrevIndex {
			prevIndex = r.nextIndex[id] - 1
			// 给 Follower 发送缺失的日志，发送的日志的索引一直递增到最新
			args := AppendEntry{
				term: r.term,
				leaderId: r.me,
				prevLogIndex: prevIndex,
				prevLogTerm: r.entries[prevIndex].term,
				leaderCommit: r.commitIndex,
				entries: r.entries[prevIndex:r.nextIndex[id]],
			}
			res := &AppendEntryReply{}
			err = client.Call("raft.AppendEntries", args, res)
			if err != nil {
				log.Println(err)
			}
			if res.term > r.term {
				// 如果任期数小，降级为 Follower
				r.roleType = Follower
				r.voteMap = make(map[int]bool)
				err = client.Close()
				if err != nil {
					log.Println(err)
				}
				break
			}
			if res.success {
				break
			}

			// 向前继续查找 Follower 缺少的第一条日志的索引
			r.nextIndex[id] = r.nextIndex[id] + 1
			r.matchIndex[id] = r.nextIndex[id] - 1
		}

		successCnt += 1
		err = client.Close()
		if err != nil {
			log.Println(err)
		}
	}

	if successCnt >= len(r.peers) / 2 {
		// 新日志成功发送到过半 Follower 节点
		r.commitIndex += 1
		applyRes, err := r.fsm.Apply(request.data)
		if err != nil {
			return err
		}
		response.data = applyRes
		return nil
	}

	return nil
}

// 客户端调用此方法来应用日志条目
func (r *raft) Apply(data []byte) error {
	// 将请求重定向到 Leader
	if r.leader == "" {
		return errors.New("找不到 Leader 节点")
	}
	r.mu.Lock()
	leaderAddr := r.leader
	r.mu.Unlock()

	client, err := rpc.Dial("tcp", string(leaderAddr))
	if err != nil {
		return err
	}
	defer func() {
		if err = client.Close();err != nil {
			log.Println(err)
		}
	}()

	args := ClientRequest{data: data}
	res := &ClientResponse{}
	// 调用 Leader 节点 rpc 接口
	err = client.Call("raft.Request", args, res)
	if err != nil {
		return err
	}
	return nil
}
