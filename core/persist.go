package core

import "encoding/gob"

type HardStatePersister interface {
	SaveHardState(*HardState) error
	LoadHardState() (*HardState, error)
}

type SnapshotPersister interface {
	SaveSnapshot(Snapshot) error
	LoadSnapshot() (Snapshot, error)
}

type Persister interface {
	HardStatePersister
	SnapshotPersister
}

type Snapshot struct {
	LastIndex int
	LastTerm  int
	Data     []byte
}

// 持久化器的默认实现，保存在文件中
type DefaultPersister struct {
	FilePath string
}

func NewDefaultPersister(fsm Fsm) *DefaultPersister {
	gob.Register(fsm)
	dp := new(DefaultPersister)
	dp.FilePath = "./persist.store"
	return dp
}

func (d *DefaultPersister) SaveHardState(state *HardState) error {
	panic("implement me")
}

func (d *DefaultPersister) LoadHardState() (*HardState, error) {
	panic("implement me")
}

func (d *DefaultPersister) SaveSnapshot(snapshot Snapshot) error {
	panic("implement me")
}

func (d *DefaultPersister) LoadSnapshot() (Snapshot, error) {
	panic("implement me")
}
