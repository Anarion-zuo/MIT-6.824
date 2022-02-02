package raft

import "log"

type AEReplied struct {
	server  int
	count   int
	success bool
	log     *LogStateMachine
}

func (trans *AEReplied) getName() string {
	return "AEReplied"
}

func (trans *AEReplied) isRW() bool {
	return true
}

func (rf *Raft) makeAEReplied(server int, count int, success bool) *AEReplied {
	return &AEReplied{
		server:  server,
		count:   count,
		success: success,
		log:     rf.log,
	}
}

func (trans *AEReplied) doSuccess() {
	// If successful: update nextIndex and matchIndex for follower
	trans.log.raft.machine.rwmu.RLock()
	trans.log.raft.print("increment %d follower nextIndex by %d", trans.server, trans.count)
	trans.log.raft.machine.rwmu.RUnlock()
	trans.log.nextIndex[trans.server] += trans.count
	trans.log.matchIndex[trans.server] = trans.log.nextIndex[trans.server] - 1
	trans.log.tryCommit()
}

func (trans *AEReplied) doFailed() {
	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry
	trans.log.raft.machine.rwmu.RLock()
	trans.log.raft.print("log rejected by %d, try again next cycle", trans.server)
	trans.log.raft.machine.rwmu.RUnlock()
	trans.log.nextIndex[trans.server]--
}

func (trans *AEReplied) transfer(source SMState) SMState {
	if source != logNormalState {
		log.Fatalln("log not at normal state")
	}
	if trans.success {
		trans.doSuccess()
	} else {
		trans.doFailed()
	}
	trans.log.raft.machine.rwmu.RLock()
	trans.log.raft.print("nextIndex %v", trans.log.nextIndex)
	trans.log.raft.machine.rwmu.RUnlock()
	return notTransferred
}

func (sm *LogStateMachine) tryCommit() {
	Ntemp := sm.commitIndex + 1
	if Ntemp > sm.lastLogIndex() {
		return
	}

	sm.raft.machine.rwmu.RLock()
	defer sm.raft.machine.rwmu.RUnlock()

	oldCommit := sm.commitIndex

	for {
		agreeCount := 0
		for i := 0; i < sm.raft.peerCount(); i++ {
			if i == sm.raft.me {
				continue
			}
			if sm.matchIndex[i] >= Ntemp && sm.getEntry(Ntemp).Term == sm.raft.machine.currentTerm {
				agreeCount++
			}
		}
		if agreeCount+1 > sm.raft.peerCount()/2 {
			sm.commitIndex = Ntemp
		}
		Ntemp++
		if Ntemp > sm.lastLogIndex() {
			break
		}
	}
	if sm.commitIndex > oldCommit {
		sm.raft.print("commitIndex updated")
		//sm.issueTransfer(sm.raft.makeApplyNew())
		sm.tryApply()
	}
}
