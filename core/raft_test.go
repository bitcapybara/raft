package core

import (
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

	// 设置当前节点 Learner 状态
	_ = rf.hardState.setTerm(0)

	// 模拟初次启动服务的处理
	reply := make(chan rpcReply)
	msg := rpc{
		rpcType: AppendEntryRpc,
		req: AppendEntry{
			entryType:    EntryHeartbeat,
			term:         1,
			leaderId:     "2",
			prevLogIndex: 0,
			prevLogTerm:  0,
			entries:      nil,
			leaderCommit: 0,
		},
		res: reply,
	}

	go func() {
		res := <-reply
		entryReply := res.res.(AppendEntryReply)
		if !entryReply.success {
			t.Errorf("not success")
		}

	}()

	rf.handleCommand(msg)
}