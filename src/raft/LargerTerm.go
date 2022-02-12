package raft

type LargerTerm struct {
	RaftStateTransfer
	newTerm   int
	newLeader int
}

func (trans *LargerTerm) getName() string {
	return "LargerTerm"
}

func (trans *LargerTerm) isRW() bool {
	return true
}

func (rf *Raft) makeLargerTerm(newTerm int, newLeader int) *LargerTerm {
	return &LargerTerm{RaftStateTransfer{rf.stateMachine}, newTerm, newLeader}
}

func (trans *LargerTerm) transfer(source SMState) SMState {
	// check term
	if trans.newTerm <= trans.machine.currentTerm {
		return notTransferred
	}
	trans.machine.votedFor = trans.newLeader
	trans.machine.currentTerm = trans.newTerm
	trans.machine.raft.sendAETimer.stop()
	trans.machine.raft.electionTimer.setElectionWait()
	trans.machine.raft.electionTimer.start()
	trans.machine.raft.persist()
	return followerState
}
