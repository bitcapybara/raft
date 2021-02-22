package core

import (
	"encoding/gob"
	"github.com/go-errors/errors"
	"os"
)

// 持久化器接口
type RaftStatePersister interface {
	// 每次 raft 的状态改变，都会调用此方法
	// entries 字段在变化之后进行持久化即可
	SaveRaftState(RaftState) error
	LoadRaftState() (RaftState, error)
}

type SnapshotPersister interface {
	SaveSnapshot(Snapshot) error
	LoadSnapshot() (Snapshot, error)
}

// raft 保存的数据
type RaftState struct {
	Term     int
	VotedFor NodeId
	Entries  []Entry
}

func newRaftState() RaftState {
	return RaftState{
		Term: 1,
		VotedFor: "",
		Entries: make([]Entry, 0),
	}
}

func (rs RaftState) toHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:     rs.Term,
		votedFor: rs.VotedFor,
		entries:  rs.Entries,
		persister: persister,
	}
}

// 保存的快照数据
type Snapshot struct {
	LastIndex int
	LastTerm  int
	Data      []byte
}

// RaftState 持久化器的默认实现，保存在文件中
type DefaultRaftStatePersister struct {
	path string
}

func NewRaftPersister(path string) *DefaultRaftStatePersister {
	return &DefaultRaftStatePersister{path: path}
}

func (d DefaultRaftStatePersister) SaveRaftState(state RaftState) error {
	file, err := os.OpenFile(d.path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return errors.Errorf("打开文件失败：%e\n", err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(state)
	if err != nil {
		return errors.Errorf("编码写入文件失败：%s\n", err)
	}
	return nil
}

func (d DefaultRaftStatePersister) LoadRaftState() (RaftState, error) {
	file, err := os.Open(d.path)
	if err != nil {
		return RaftState{}, errors.Errorf("打开文件失败：%e\n", err)
	}
	var state RaftState
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&state)
	if err != nil {
		return RaftState{},  errors.Errorf("文件解码读取失败：%s\n", err)
	}
	return state, nil
}

type DefaultSnapshotPersister struct {
	path string
}

func NewSnapshotPersister(path string) *DefaultSnapshotPersister {
	return &DefaultSnapshotPersister{path: path}
}

func (d DefaultSnapshotPersister) SaveSnapshot(snapshot Snapshot) error {
	file, err := os.OpenFile(d.path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return errors.Errorf("打开文件失败：%e\n", err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(snapshot)
	if err != nil {
		return errors.Errorf("编码写入文件失败：%s\n", err)
	}
	return nil
}

func (d DefaultSnapshotPersister) LoadSnapshot() (Snapshot, error) {
	file, err := os.Open(d.path)
	if err != nil {
		return Snapshot{}, errors.Errorf("打开文件失败：%e\n", err)
	}
	var state Snapshot
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&state)
	if err != nil {
		return Snapshot{},  errors.Errorf("文件解码读取失败：%s\n", err)
	}
	return state, nil
}
