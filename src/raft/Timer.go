package raft

import (
	"sync"
	"time"
)

type Timer struct {
	waitMs  int
	command SMTransfer // send this when timer expires
	raft    *Raft
	timer1  *time.Timer
	mu      sync.Mutex
}

/**
Timer
makeTimer returns a newly started timer.
*/
func makeTimer(waitMs int, command SMTransfer, rf *Raft) *Timer {
	timer := &Timer{
		waitMs:  waitMs,
		command: command,
		raft:    rf,
		//cleared: false,
	}
	// do not start timer as yet!
	//timer.start()
	timer.timer1 = time.NewTimer(10000 * time.Second)
	go func(timer *Timer) {
		timer.stop()
		for {
			<-timer.timer1.C
			timer.raft.machine.issueTransfer(timer.command)
			timer.start()
		}
	}(timer)
	return timer
}

func (timer *Timer) setWaitMs(waitMs int) {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	timer.waitMs = waitMs
}

func (timer *Timer) stop() {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if !timer.timer1.Stop() {
		select {
		case <-timer.timer1.C:
		default:
		}
	}
}

func (timer *Timer) start() {
	timer.stop()
	timer.mu.Lock()
	duration := time.Duration(timer.waitMs) * time.Millisecond
	defer timer.mu.Unlock()
	timer.timer1.Reset(duration)
}

func (timer *Timer) clear() {
	timer.start()
}
