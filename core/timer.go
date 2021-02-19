package core

import (
	"math/rand"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type timer struct {
	enable             bool        // 当前计时器是否有效
	electionTimer      *time.Timer // 选举超时计时器
	heartbeatTimer     *time.Timer // 心跳计时器

	electionMinTimeout int         // 最小选举超时时间
	electionMaxTimeout int         // 最大选举超时时间
	heartbeatTimeout   int         // 心跳间隔时间

	mu                 sync.Mutex  // 修改 enable 字段时要加锁
}

func NewTimer(config Config) *timer {
	return &timer{
		enable: true,
		electionMinTimeout: config.ElectionMinTimeout,
		electionMaxTimeout: config.ElectionMaxTimeout,
		heartbeatTimeout: config.HeartbeatTimeout,
	}
}

func (t *timer) initTimer() {
	t.electionTimer = time.NewTimer(t.newElectionDuration())
	t.heartbeatTimer = time.NewTimer(t.heartbeatDuration())
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

func (t *timer) setElectionTimer() {
	t.electionTimer.Reset(t.newElectionDuration())
}

func (t *timer) resetElectionTimer() {
	t.electionTimer.Stop()
	t.setElectionTimer()
}

func (t *timer) setHeartbeatTimer() {
	t.heartbeatTimer.Reset(t.heartbeatDuration())
}

func (t *timer) newElectionDuration() time.Duration {
	randTimeout := rand.Intn(t.electionMaxTimeout - t.electionMinTimeout) + t.electionMinTimeout
	return time.Millisecond * time.Duration(randTimeout)
}

func (t *timer) heartbeatDuration() time.Duration {
	return time.Millisecond * time.Duration(t.heartbeatTimeout)
}
