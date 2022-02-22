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

type _ApplyInfo struct {
	entries  []LogEntry
	begin    int
	isLeader bool
	term     int
}

func (sm *RaftStateMachine) applyRoutine() {
	for {
		sm.applyCond.L.Lock()
		if sm.lastApplied > sm.commitIndex {
			panic("lastApplied > commitIndex")
		}
		for sm.commitIndex == sm.lastApplied {
			sm.applyCond.Wait()
		}
		info := sm.tryApply()
		sm.applyCond.L.Unlock()
		if info != nil {
			sm.applyGiven(info.entries, info.begin, info.isLeader, info.term)
		}
	}
}

func (sm *RaftStateMachine) applyGiven(entries []LogEntry, begin int, isLeader bool, term int) {
	for i, entry := range entries {
		sm.rwmu.RLock()
		sm.raft.print("applying index %d", begin+i)
		sm.rwmu.RUnlock()
		*sm.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: begin + i,
			IsLeader:     isLeader,
			Term:         term,
		}
	}
}

// apply must be in order
// it might be that each time commitIndex is increased
// this function is called
// if it is called too frequently
// the execution may overlap
// thus causing application out of order
func (sm *RaftStateMachine) tryApply() *_ApplyInfo {
	applyLen := sm.commitIndex - sm.lastApplied
	if applyLen > 0 {
		//sm.raft.print("applying %d entries", applyLen)
		toBeSent := make([]LogEntry, sm.commitIndex-sm.lastApplied)
		copy(toBeSent, sm.log[sm.physicalIndex(sm.lastApplied+1):sm.physicalIndex(sm.commitIndex+1)])
		begin := sm.lastApplied + 1
		sm.lastApplied = sm.commitIndex
		return &_ApplyInfo{
			entries:  toBeSent,
			begin:    begin,
			isLeader: sm.curState == sendAEState,
			term:     sm.currentTerm,
		}
	}
	return nil
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
	//trans.machine.tryApply()
	trans.machine.applyCond.Broadcast()

	trans.machine.raft.persist()

	return notTransferred
}
