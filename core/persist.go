package core

import (
	"encoding/gob"
	"fmt"
	"github.com/go-errors/errors"
	"os"
	"strings"
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
		Term:     1,
		VotedFor: "",
		Entries:  make([]Entry, 0),
	}
}

func (rs RaftState) toHardState(persister RaftStatePersister) HardState {
	return HardState{
		term:      rs.Term,
		votedFor:  rs.VotedFor,
		entries:   rs.Entries,
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
	dirPath  string
	filename string
}

func NewRaftPersister(dirPath string) DefaultRaftStatePersister {
	return DefaultRaftStatePersister{
		dirPath:  dirPath,
		filename: "raftState.store",
	}
}

func (d DefaultRaftStatePersister) SaveRaftState(state RaftState) error {
	// 检查文件夹，不存在则创建
	err := checkDir(d.dirPath)
	if err != nil {
		return fmt.Errorf("检查文件夹发生错误：%w", err)
	}
	// 数据写入
	file, err := os.OpenFile(buildFilePath(d.dirPath, d.filename), os.O_WRONLY|os.O_CREATE, os.ModePerm)
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
	filePath := buildFilePath(d.dirPath, d.filename)
	// 检查文件是否存在
	fileExists, err := checkFile(filePath)
	if err != nil {
		return RaftState{}, fmt.Errorf("检查文件夹发生错误：%w", err)
	}
	if !fileExists {
		return RaftState{}, fmt.Errorf("raftState 数据文件不存在！")
	}
	// 读取文件
	file, err := os.Open(filePath)
	if err != nil {
		return RaftState{}, errors.Errorf("打开文件失败：%e\n", err)
	}
	var state RaftState
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&state)
	if err != nil {
		return RaftState{}, errors.Errorf("文件解码读取失败：%s\n", err)
	}
	return state, nil
}

type DefaultSnapshotPersister struct {
	dirPath  string
	filename string
}

func NewSnapshotPersister(dirPath string) DefaultSnapshotPersister {
	return DefaultSnapshotPersister{dirPath: dirPath, filename: "snapshot.store"}
}

func (d DefaultSnapshotPersister) SaveSnapshot(snapshot Snapshot) error {
	// 检查文件夹，不存在则创建
	err := checkDir(d.dirPath)
	if err != nil {
		return fmt.Errorf("检查文件夹发生错误：%w", err)
	}
	// 数据写入
	file, err := os.OpenFile(buildFilePath(d.dirPath, d.filename), os.O_WRONLY|os.O_CREATE, os.ModePerm)
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
	filePath := buildFilePath(d.dirPath, d.filename)
	// 检查数据文件是否存在
	fileExists, err := checkFile(filePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("检查文件发生错误：%w", err)
	}
	if !fileExists {
		return Snapshot{}, fmt.Errorf("snapshot 数据文件不存在！")
	}
	// 读取文件
	file, err := os.Open(filePath)
	if err != nil {
		return Snapshot{}, errors.Errorf("打开文件失败：%e\n", err)
	}
	var state Snapshot
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&state)
	if err != nil {
		return Snapshot{}, errors.Errorf("文件解码读取失败：%s\n", err)
	}
	return state, nil
}

func checkDir(dirPath string) error {

	// 是否存在此目录
	hasDir := false
	stat, err := os.Stat(dirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		hasDir = stat.IsDir()
	}

	// 不存在此目录，则创建
	if !hasDir {
		err := os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			return fmt.Errorf("创建文件夹失败：%w", err)
		}
	}

	return nil
}

func checkFile(filePath string) (bool, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		return false, fmt.Errorf("读取文件元数据失败：%w", err)
	}
	return !stat.IsDir(), err
}

func buildFilePath(dirPath, filename string) string {
	return strings.TrimRight(dirPath, "/") + "/" + filename
}
