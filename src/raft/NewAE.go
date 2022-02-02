package raft

import "log"

type NewAE struct {
	prevLogIndex int
	entries      *[]LogEntry
	leaderCommit int
	log          *LogStateMachine
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
		log:          rf.log,
	}
}

func (trans *NewAE) transfer(source SMState) SMState {
	if source != logNormalState {
		log.Fatalln("log not at normal state")
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	trans.log.removeAfter(trans.prevLogIndex + 1)
	// Append any new entries not already in the log
	trans.log.raft.print("appending %d entries", len(*trans.entries))
	trans.log.appendLog(trans.entries)
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if trans.leaderCommit > trans.log.commitIndex {
		trans.log.raft.machine.rwmu.RLock()
		trans.log.raft.print("leader commit %d larger than mine %d", trans.leaderCommit, trans.log.commitIndex)
		trans.log.raft.machine.rwmu.RUnlock()
		trans.log.commitIndex = trans.log.lastLogIndex()
		if trans.leaderCommit < trans.log.lastLogIndex() {
			trans.log.commitIndex = trans.leaderCommit
		}
	}
	trans.log.tryApply()
	return notTransferred
}
