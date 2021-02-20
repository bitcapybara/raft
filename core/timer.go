package core

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==================== timerManager ====================

type timerManager struct {
	electionTimer  *time.Timer       // 选举超时计时器
	heartbeatTimer map[NodeId]*time.Timer // 各 Follower 维护一个计时器

	electionMinTimeout int // 最小选举超时时间
	electionMaxTimeout int // 最大选举超时时间
	heartbeatTimeout   int // 心跳间隔时间
}

func NewTimerManager(config Config) *timerManager {
	return &timerManager{
		electionMinTimeout: config.ElectionMinTimeout,
		electionMaxTimeout: config.ElectionMaxTimeout,
		heartbeatTimeout:   config.HeartbeatTimeout,
	}
}

func (t *timerManager) initTimerManager(peers map[NodeId]NodeAddr) {
	t.electionTimer = time.NewTimer(t.newElectionDuration())
	hbTimerMap := t.heartbeatTimer
	for id, _ := range peers {
		hbTimerMap[id] = time.NewTimer(t.heartbeatDuration())
	}
}

func (t *timerManager) setElectionTimer() {
	t.electionTimer.Reset(t.newElectionDuration())
}

func (t *timerManager) resetElectionTimer() {
	t.electionTimer.Stop()
	t.setElectionTimer()
}

func (t *timerManager) stopHeartbeatTimer(id NodeId) {
	t.heartbeatTimer[id].Stop()
}

func (t *timerManager) setHeartbeatTimer(id NodeId) {
	t.heartbeatTimer[id].Reset(t.heartbeatDuration())
}

func (t *timerManager) newElectionDuration() time.Duration {
	randTimeout := rand.Intn(t.electionMaxTimeout-t.electionMinTimeout) + t.electionMinTimeout
	return time.Millisecond * time.Duration(randTimeout)
}

func (t *timerManager) heartbeatDuration() time.Duration {
	return time.Millisecond * time.Duration(t.heartbeatTimeout)
}
