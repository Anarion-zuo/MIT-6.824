package raft

type NewAE struct {
	prevLogIndex int
	entries      []LogEntry
	leaderCommit int
	machine      *RaftStateMachine
}

func (trans *NewAE) getName() string {
	return "NewAE"
}

func (trans *NewAE) isRW() bool {
	return true
}

func (rf *Raft) makeNewAE(prevLogIndex int, entries []LogEntry, leaderCommit int) *NewAE {
	return &NewAE{
		prevLogIndex: prevLogIndex,
		entries:      entries,
		leaderCommit: leaderCommit,
		machine:      rf.stateMachine,
	}
}

//type _ApplyInfo struct {
//	entries   []LogEntry
//	begin     int
//	isLeader  bool
//	term      int
//	stateSize int
//}

func (sm *RaftStateMachine) sendToApplyQ(msg *ApplyMsg) {
	sm.applyCond.L.Lock()
	sm.applyQ.PushBack(msg)
	sm.applyCond.Broadcast()
	sm.applyCond.L.Unlock()
}

func (sm *RaftStateMachine) pollApplyQ() *ApplyMsg {
	sm.applyCond.L.Lock()
	defer sm.applyCond.L.Unlock()
	for sm.applyQ.Len() == 0 {
		sm.applyCond.Wait()
	}
	front := sm.applyQ.Front()
	result := front.Value.(*ApplyMsg)
	sm.applyQ.Remove(front)
	return result
}

func (sm *RaftStateMachine) pollApplyQRoutine() {
	for {
		msg := sm.pollApplyQ()
		sm.rwmu.RLock()
		if msg.CommandValid {
			sm.raft.print("applying command index %d", msg.CommandIndex)
		} else if msg.SnapshotValid {
			sm.raft.print("applying snapshot index %d", msg.SnapshotIndex)
		} else {
			panic("unknown apply type")
		}
		sm.rwmu.RUnlock()
		sm.applyCh <- *msg
	}
}

//func (sm *RaftStateMachine) applyRoutine() {
//	for {
//		sm.applyCond.L.Lock()
//		if sm.lastApplied > sm.commitIndex {
//			panic("lastApplied > commitIndex")
//		}
//		for sm.commitIndex == sm.lastApplied {
//			sm.applyCond.Wait()
//		}
//		info := sm.tryApply()
//		sm.applyCond.L.Unlock()
//		if info != nil {
//			sm.applyGiven(info.entries, info.begin, info.isLeader, info.term, info.stateSize)
//		}
//	}
//}

//func (sm *RaftStateMachine) applyGiven(entries []LogEntry, begin int, isLeader bool, term int, stateSize int) {
//	for i, entry := range entries {
//		sm.rwmu.RLock()
//		sm.raft.print("applying index %d", begin+i)
//		sm.rwmu.RUnlock()
//		sm.applyCh <- ApplyMsg{
//			CommandValid: true,
//			Command:      entry.Command,
//			CommandIndex: begin + i,
//			IsLeader:     isLeader,
//			Term:         term,
//			StateSize:    stateSize,
//		}
//	}
//}

// apply must be in order
// it might be that each time commitIndex is increased
// this function is called
// if it is called too frequently
// the execution may overlap
// thus causing application out of order
func (sm *RaftStateMachine) tryApply() {
	for i := sm.lastApplied + 1; i <= sm.commitIndex; i++ {
		sm.raft.print("sending index %d to applyQ", i)
		sm.sendToApplyQ(&ApplyMsg{
			CommandValid: true,
			Command:      sm.getEntry(i).Command,
			CommandIndex: i,
			IsLeader:     sm.curState == sendAEState,
			Term:         sm.currentTerm,
			StateSize:    sm.raft.persister.RaftStateSize(),
		})
	}
	sm.lastApplied = sm.commitIndex
	//applyLen := sm.commitIndex - sm.lastApplied
	//if applyLen > 0 {
	//	//sm.raft.print("applying %d entries", applyLen)
	//	toBeSent := make([]LogEntry, sm.commitIndex-sm.lastApplied)
	//	copy(toBeSent, sm.log[sm.physicalIndex(sm.lastApplied+1):sm.physicalIndex(sm.commitIndex+1)])
	//	begin := sm.lastApplied + 1
	//	sm.lastApplied = sm.commitIndex
	//	return &_ApplyInfo{
	//		entries:   toBeSent,
	//		begin:     begin,
	//		isLeader:  sm.curState == sendAEState,
	//		term:      sm.currentTerm,
	//		stateSize: sm.raft.persister.RaftStateSize(),
	//	}
	//}
	//return nil
}

func (trans *NewAE) transfer(source SMState) SMState {
	//if source != logNormalState {
	//	log.Fatalln("log not at normal state")
	//}
	trans.machine.raft.print("appending %d entries", len(trans.entries))
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	trans.machine.removeAfter(trans.prevLogIndex + 1)
	// Append any new entries not already in the log
	trans.machine.appendLog(trans.entries...)
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
	//trans.machine.applyCond.Broadcast()

	trans.machine.raft.persist()

	return notTransferred
}
