package core

import (
	"fmt"
	"sync"
	"testing"
)

// 测试用客户端状态机
type testFsm struct {
}

func newTestFsm() *testFsm {
	return &testFsm{}
}

func (f *testFsm) Apply(bytes []byte) error {
	println("testFsm Apply...")
	return nil
}

func (f *testFsm) Serialize() ([]byte, error) {
	return nil, nil
}

func TestHandleCommand(t *testing.T) {
	config := Config{
		Fsm: newTestFsm(),
		RaftStatePersister: newImMemRaftStatePersister(),
		SnapshotPersister: newInMemSnapshotPersister(),
		Transport: newInMemTransport(),
		Peers: map[NodeId]NodeAddr{"1": "a", "2": "b", "3": "c", "4": "d", "5": "e"},
		Me: "1",
		ElectionMaxTimeout: 2000,
		ElectionMinTimeout: 1000,
		HeartbeatTimeout: 100,
		MaxLogLength: 2000,
	}
	rf := newRaft(config)
	_ = rf.hardState.setTerm(1)

	reply := make(chan rpcReply)
	msgs := []rpc{
		{
			rpcType: AppendEntryRpc,
			req: AppendEntry{
				entryType:    EntryHeartbeat,
				term:         1,
				leaderId:     "2",
				prevLogIndex: 0,
				prevLogTerm:  0,
				entries:      []Entry{{Index: 0, Term: 1, Type: EntryHeartbeat}},
				leaderCommit: 0,
			},
			res: reply,
		},{
			rpcType: AppendEntryRpc,
			req: AppendEntry{
				entryType:    EntryReplicate,
				term:         1,
				leaderId:     "2",
				prevLogIndex: 0,
				prevLogTerm:  0,
				entries:      []Entry{{Index: 0, Term: 1, Type: EntryReplicate, Data: []byte("a")}},
				leaderCommit: 0,
			},
			res: reply,
		},
	}

	var wg sync.WaitGroup
	getReply := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := <-reply
			entryReply := res.res.(AppendEntryReply)
			if !entryReply.success {
				t.Errorf("not success")
			} else {
				fmt.Printf("term=%d, leader=%s, entry_size=%d\n",
					rf.hardState.currentTerm(),
					rf.peerState.leaderId(),
					rf.hardState.logLength())
			}
		}()
	}

	// 模拟初次启动服务的处理
	getReply()
	rf.handleCommand(msgs[0])
	wg.Wait()

	// 模拟第一次发送日志
	getReply()
	rf.handleCommand(msgs[1])
	wg.Wait()
}