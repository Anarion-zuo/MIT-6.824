package raft

import (
	"github.com/sasha-s/go-deadlock"
)

type StateMachine struct {
	curState SMState
	transCh  chan SMTransfer

	rwmu deadlock.RWMutex
}

func (sm *StateMachine) machineLoop() {
	for {
		trans := <-sm.transCh
		//fmt.Println("sm: execute " + trans.getName())
		/*if trans.isRW() {
			sm.rwmu.Lock()
			sm.callTransfer(trans)
			sm.rwmu.Unlock()
		} else {
			sm.rwmu.RLock()
			dest := trans.transfer(sm.curState)
			sm.rwmu.RUnlock()
			if dest != notTransferred {
				sm.rwmu.Lock()
				sm.curState = dest
				sm.rwmu.Unlock()
			}
		}*/
		sm.rwmu.Lock()
		sm.callTransfer(trans)
		sm.rwmu.Unlock()
	}
}

func (sm *StateMachine) issueTransfer(trans SMTransfer) {
	go func() {
		sm.transCh <- trans
	}()
}

func (sm *StateMachine) callTransfer(trans SMTransfer) {
	dest := trans.transfer(sm.curState)
	if dest != notTransferred {
		sm.curState = dest
	}
}

type SMState int

const notTransferred SMState = -1

type SMTransfer interface {
	transfer(source SMState) SMState
	getName() string
	isRW() bool
}
