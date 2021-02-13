package core

import (
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
	leaderAddr   string  // 领导者的地址，方便 follower 重定向
	prevLogIndex int     // 紧接新记录之前的日志条目索引
	prevLogTerm  int     // prevLogIndex 条目所处任期
	entries      []Entry // 日志条目（心跳为空；为提高效率可能发送多个）
	leaderCommit int     // 领导者提交的索引
}

type AppendEntryRes struct {
	term    int  // 当前时刻所属任期，用于领导者更新自身
	success bool // 如果关注者包含与prevLogIndex和prevLogTerm匹配的条目，则为true
}

type RequestVote struct {
	term          int    // 当前时刻所属任期
	candidateAddr string // 候选人地址
	lastLogIndex  int    // 候选人最后一个日志条目的索引
	lastLogTerm   int    // lastLogIndex 所处的任期
}

type RequestVoteRes struct {
	term        int  // 当前时刻所属任期，用于领导者更新自身
	voteGranted bool // 为 true 表示候选人收到一个选票
}

type raft struct {
	// 当前节点的角色
	roleType roleType
	// 所有节点
	peers []string
	// 当前节点在 peers 中的索引
	me int
	// 当前时刻所处的 term
	term int
	// 当前节点为 follower/candidate 时，为选举超时计时器
	// 当前节点为 leader 时，为心跳计时器
	timer *time.Timer
	// 选举投票记录
	voteMap map[int]bool

	mu sync.Mutex
}

func New(peers []string, me int) *raft {
	rf := new(raft)
	rf.roleType = Follower
	rf.peers = peers
	rf.me = me

	rf.term = -1
	rf.voteMap = make(map[int]bool)
	return rf
}

func (r *raft) Start() {
	// 新建 rf 对象
	rf := New([]string{}, 0)
	// 设定计时器，开始后台线程
	go r.loop()

	// 开放 rpc 服务接口
	rpcServer := rpc.NewServer()
	err := rpcServer.Register(rf)
	if err != nil {
		log.Fatalf("开启 rpc 服务失败！%s", err)
	}
	// 开启监听器
	listener, err := net.Listen("tcp", r.peers[r.me])
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
		for index, peer := range r.peers {
			if index == r.me {
				continue
			}
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				log.Printf("创建rpc客户端失败：%s%s\n", peer, err)
				continue
			}
			args := AppendEntry{
				term:         r.term,
				leaderAddr:   r.peers[r.me],
				prevLogIndex: 0,
				prevLogTerm:  0,
				entries:      nil,
				leaderCommit: 0,
			}
			res := &AppendEntryRes{}
			err = client.Call("raft.requestVote", args, res)
			if err != nil {
				log.Printf("调用rpc服务失败：%s%s\n", peer, err)
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
		term := r.term + 1
		for index, peer := range r.peers {
			if index == r.me {
				vote += 1
				continue
			}
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				log.Printf("创建rpc客户端失败：%s%s\n", peer, err)
				continue
			}
			args := RequestVote{
				term:          term,
				candidateAddr: r.peers[r.me],
			}
			res := &RequestVoteRes{}
			err = client.Call("raft.requestVote", args, res)
			if err != nil {
				log.Printf("调用rpc服务失败：%s%s\n", peer, err)
			}
			if res.term <= term && res.voteGranted {
				// 成功获得选票
				vote += 1
			} else if res.term > term {
				// 当前节点任期落后，则退出竞选
				r.roleType = Follower
				r.term = res.term
				break
			}
		}
		if vote >= len(r.peers)/2 {
			r.roleType = Leader
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
func (r *raft) appendEntries(args AppendEntry, res *AppendEntryRes) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// ========== 接收日志条目 ==========
	// todo 日志复制

	// ========== 接收心跳 ==========
	if r.roleType != Leader && args.term >= r.term {
		// follower/candidate 接收到心跳，重置计时器
		r.roleType = Follower
		r.setTimer(300, 500)
	}
	res.term = r.term
	res.success = false
	return nil
}

// follower 和 candidate 开放的 rpc接口，由 candidate 调用
func (r *raft) requestVote(args RequestVote, res *RequestVoteRes) error {
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
