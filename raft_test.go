package raft

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
		Fsm:                newTestFsm(),
		RaftStatePersister: newImMemRaftStatePersister(),
		SnapshotPersister:  newInMemSnapshotPersister(),
		Transport:          newInMemTransport(),
		Peers:              map[NodeId]NodeAddr{"1": "a", "2": "b", "3": "c", "4": "d", "5": "e"},
		Me:                 "1",
		ElectionMaxTimeout: 2000,
		ElectionMinTimeout: 1000,
		HeartbeatTimeout:   100,
		MaxLogLength:       2000,
	}
	rf := newRaft(config)
	_ = rf.hardState.setTerm(1)

	reply := make(chan rpcReply)
	msgs := []rpc{
		{ // 模拟初次启动服务的处理
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
		},
	}

	var wg sync.WaitGroup
	getReply := func(success bool, role RoleStage, term int, leader NodeId, entrySize int, commit int, applied int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := <-reply
			entryReply := res.res.(AppendEntryReply)

			replySuccess := entryReply.success
			roleStage := rf.roleState.getRoleStage()
			currentTerm := rf.hardState.currentTerm()
			leaderId := rf.peerState.leaderId()
			logLength := rf.logLength()
			commitIndex := rf.softState.getCommitIndex()
			lastApplied := rf.softState.getLastApplied()

			sprintf := fmt.Sprintf("success=%v(%v), role=%d(%d), term=%d(%d), leader=%s(%s), " +
				"entry_size=%d(%d), commit=%d(%d), applied=%d(%d)\n",
				replySuccess, success, roleStage, role, currentTerm, term, leaderId, leader,
				logLength, entrySize, commitIndex, commit, lastApplied, applied)

			if replySuccess != success || roleStage != role || currentTerm != term || leaderId != leader ||
				logLength != entrySize || commitIndex != commit || lastApplied != applied {
				t.Errorf(sprintf)
			} else {
				fmt.Printf(sprintf)
			}
		}()
	}

	getReply(true, Learner, 1, "2", 0, 0, 0)
	rf.handleCommand(msgs[0])
	wg.Wait()
}
