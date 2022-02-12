package raft

type NewAE struct {
	prevLogIndex int
	entries      *[]LogEntry
	leaderCommit int
	machine      *RaftStateMachine
}

func (trans *NewAE) getName() string {
	return "NewAE"
}

func (trans *NewAE) isRW() bool {
	return true
}

func (rf *Raft) makeNewAE(prevLogIndex int, entries *[]LogEntry, leaderCommit int) *NewAE {
	return &NewAE{
		prevLogIndex: prevLogIndex,
		entries:      entries,
		leaderCommit: leaderCommit,
		machine:      rf.stateMachine,
	}
}

func (sm *RaftStateMachine) tryApplyRoutine(entries *[]LogEntry, begin int) {
	for i, entry := range *entries {
		*sm.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: begin + i,
			// TODO snap
		}
	}
}

func (sm *RaftStateMachine) tryApply() {
	applyLen := sm.commitIndex - sm.lastApplied
	if applyLen > 0 {
		//sm.raft.print("applying %d entries", applyLen)
		toBeSent := make([]LogEntry, sm.commitIndex-sm.lastApplied)
		copy(toBeSent, sm.log[sm.lastApplied+1:sm.commitIndex+1])
		begin := sm.lastApplied + 1
		sm.lastApplied = sm.commitIndex
		go sm.tryApplyRoutine(&toBeSent, begin)
	}
}

func (trans *NewAE) transfer(source SMState) SMState {
	//if source != logNormalState {
	//	log.Fatalln("log not at normal state")
	//}
	trans.machine.raft.print("appending %d entries", len(*trans.entries))
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	trans.machine.removeAfter(trans.prevLogIndex + 1)
	// Append any new entries not already in the log
	trans.machine.appendLog(*trans.entries...)
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if trans.leaderCommit > trans.machine.commitIndex {
		trans.machine.raft.print("leader commit %d larger than mine %d", trans.leaderCommit, trans.machine.commitIndex)
		trans.machine.commitIndex = trans.machine.lastLogIndex()
		if trans.leaderCommit < trans.machine.lastLogIndex() {
			trans.machine.commitIndex = trans.leaderCommit
		}
	}
	trans.machine.tryApply()

	trans.machine.raft.persist()

	return notTransferred
}
