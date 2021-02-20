package core

import (
	"math/rand"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==================== timer ====================

type timer struct {
	enable bool
	timer  *time.Timer
	mu     sync.Mutex
}

func (t *timer) enableTimer() {
	t.mu.Lock()
	t.enable = true
	t.mu.Unlock()
}

func (t *timer) disableTimer() {
	t.mu.Lock()
	t.enable = false
	t.mu.Unlock()
}

func (t *timer) isEnable() bool {
	t.mu.Lock()
	enable := t.enable
	t.mu.Unlock()
	return enable
}

func (t *timer) setTimer(duration time.Duration) {
	t.timer.Reset(duration)
}

// ==================== timerManager ====================

type timerManager struct {
	electionTimer  *time.Timer       // 选举超时计时器
	heartbeatTimer map[NodeId]*timer // 各 Follower 维护一个计时器

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
		hbTimerMap[id] = &timer{
			enable: true,
			timer: time.NewTimer(t.heartbeatDuration()),
		}
	}
}

func (t *timerManager) setElectionTimer() {
	t.electionTimer.Reset(t.newElectionDuration())
}

func (t *timerManager) resetElectionTimer() {
	t.electionTimer.Stop()
	t.setElectionTimer()
}

func (t *timerManager) setHeartbeatTimer(id NodeId) {
	t.heartbeatTimer[id].setTimer(t.heartbeatDuration())
}

func (t *timerManager) enableHeartbeatTimer(id NodeId) {
	t.heartbeatTimer[id].enableTimer()
}

func (t *timerManager) disableHeartbeatTimer(id NodeId) {
	t.heartbeatTimer[id].disableTimer()
}

func (t *timerManager) newElectionDuration() time.Duration {
	randTimeout := rand.Intn(t.electionMaxTimeout-t.electionMinTimeout) + t.electionMinTimeout
	return time.Millisecond * time.Duration(randTimeout)
}

func (t *timerManager) heartbeatDuration() time.Duration {
	return time.Millisecond * time.Duration(t.heartbeatTimeout)
}
