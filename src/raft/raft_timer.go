package raft

import (
	"math/rand"
	"time"
)

const (
	LEAST_WAIT_TIME = 400
	MAX_WAIT_TIME = 800
)

const (
	HEART_BEAT_INTERVAL time.Duration = time.Microsecond * 100
	RPC_CALL_TIMEOUT time.Duration = time.Second * 1
)

type RTimer struct {
	timer *time.Timer
}

func NewRTimer() *RTimer {
	return &RTimer{timer: time.NewTimer(getWaitTime())}
}

func (r *RTimer) ResetTimer() {
	r.timer.Reset(getWaitTime())
}

func getWaitTime() time.Duration {
	length := 400 + rand.Intn(4) * 100
	return time.Millisecond * time.Duration(length)
}