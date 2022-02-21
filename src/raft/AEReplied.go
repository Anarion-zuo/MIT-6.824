package raft

type AEReplied struct {
	server  int
	args    *AppendEntriesArgs
	reply   *AppendEntriesReply
	machine *RaftStateMachine
}

func (trans *AEReplied) getName() string {
	return "AEReplied"
}

func (trans *AEReplied) isRW() bool {
	return true
}

func (rf *Raft) makeAEReplied(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) *AEReplied {
	return &AEReplied{
		server:  server,
		args:    args,
		reply:   reply,
		machine: rf.stateMachine,
	}
}

func (trans *AEReplied) doSuccess() {
	// If successful: update nextIndex and matchIndex for follower
	trans.machine.raft.print("increment %d follower nextIndex by %d", trans.server, len(trans.args.Entries))
	trans.machine.nextIndex[trans.server] = trans.args.PrevLogIndex + 1 + len(trans.args.Entries)
	trans.machine.matchIndex[trans.server] = trans.machine.nextIndex[trans.server] - 1
	trans.machine.tryCommit()
}

func (trans *AEReplied) doFailed() {
	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry
	//trans.logMachine.nextIndex[trans.server]--

	// fast backtracking
	if trans.reply.ConflictPrevIndex < 1 {
		trans.machine.nextIndex[trans.server] = 1
	} else {
		trans.machine.nextIndex[trans.server] = trans.reply.ConflictPrevIndex
	}
	//trans.machine.nextIndex[trans.server] = trans.conflictPrevIndex + 1

	trans.machine.raft.print("log rejected by %d, try again on nextIndex %d next cycle", trans.server, trans.machine.nextIndex[trans.server])

}

func (trans *AEReplied) transfer(source SMState) SMState {
	if trans.reply.Success {
		trans.doSuccess()
	} else {
		trans.doFailed()
	}
	trans.machine.raft.print("nextIndex %v", trans.machine.nextIndex)
	trans.machine.raft.persist()
	return notTransferred
}

func (sm *RaftStateMachine) tryCommit() {
	Ntemp := sm.commitIndex + 1
	if Ntemp > sm.lastLogIndex() {
		return
	}

	oldCommit := sm.commitIndex

	for {
		agreeCount := 0
		for i := 0; i < sm.raft.PeerCount(); i++ {
			if i == sm.raft.me {
				continue
			}
			if sm.matchIndex[i] >= Ntemp && sm.getEntry(Ntemp).Term == sm.raft.stateMachine.currentTerm {
				agreeCount++
			}
		}
		if agreeCount+1 > sm.raft.PeerCount()/2 {
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
		//sm.tryApply()
		sm.applyCond.Broadcast()
	}
}
