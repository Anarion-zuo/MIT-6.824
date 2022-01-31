package raft

import "sync"

type StateMachine struct {
	curState         SMState
	transCh          chan SMTransfer
	transferExecutor TransferExecutor
	stateWriter      StateWriter

	rwmu sync.RWMutex
}

type TransferExecutor interface {
	executeTransfer(source SMState, trans SMTransfer) SMState
}

type StateWriter interface {
	writeState(state SMState)
}

type DefaultExecutor struct {
}

func (e *DefaultExecutor) executeTransfer(source SMState, trans SMTransfer) SMState {
	return trans.transfer(source)
}

func (sm *StateMachine) machineLoop() {
	for {
		trans := <-sm.transCh
		sm.rwmu.Lock()
		dest := sm.transferExecutor.executeTransfer(sm.curState, trans)
		if dest != notTransferred {
			sm.stateWriter.writeState(dest)
		}
		sm.rwmu.Unlock()
	}
}

type DefaultStateWriter struct {
	machine StateMachine
}

func (sw *DefaultStateWriter) writeState(dest SMState) {
	sw.machine.curState = dest
}

func (sm *StateMachine) issueTransfer(trans SMTransfer) {
	go func() {
		sm.transCh <- trans
	}()
}

type SMState int

const notTransferred SMState = -1

type SMTransfer interface {
	transfer(source SMState) SMState
	getName() string
}
