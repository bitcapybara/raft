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

	reply := make(chan rpcReply)
	msg := rpc{
		rpcType: AppendEntryRpc,
		req: AppendEntry{

		},
		res: reply,
	}
	rf.handleCommand(msg)

	res := <-reply
	println("%s", res.res)
}