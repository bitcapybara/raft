package core

import (
	"testing"
)

func TestDefaultRaftStatePersister_SaveRaftState(t *testing.T) {

	raftState := RaftState{
		Term: 1,
		VotedFor: "node1",
		Entries: make([]Entry, 0),
	}

	raftState.Entries = append(raftState.Entries, Entry{Index: 1, Term: 1, Data: []byte("测试数据testing")})

	persister := NewRaftPersister("../data/raftState.store")
	err := persister.SaveRaftState(raftState)
	if err != nil {
		t.Errorf("保存 RaftState 测试失败：%s\n", err.Error())
	}
}

func TestDefaultRaftStatePersister_LoadRaftState(t *testing.T) {

	persister := NewRaftPersister("../data/raftState.store")
	raftState, err := persister.LoadRaftState()
	if err != nil {
		t.Errorf("读取 RaftState 测试失败：%s\n", err)
	}
	t.Log(raftState)
	t.Log(string(raftState.Entries[0].Data))
}
