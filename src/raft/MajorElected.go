package raft

import "sync"

type MajorElected struct {
	RaftStateTransfer
}

func (trans *MajorElected) getName() string {
	return "MajorElected"
}

func (trans *MajorElected) isRW() bool {
	return false
}

func (rf *Raft) makeMajorElected() *MajorElected {
	return &MajorElected{RaftStateTransfer{machine: rf.machine}}
}

func (trans *MajorElected) transfer(source SMState) SMState {
	// check state
	if source != startElectionState && source != sendAEState {
		return notTransferred
	}
	trans.machine.raft.electionTimer.stop()
	// start send AE
	go trans.machine.raft.sendAEs()
	// set timer
	trans.machine.raft.sendAETimer.start()

	return sendAEState
}

func (rf *Raft) sendSingleAE(server int, joinCount *int, cond *sync.Cond) {
	rf.machine.rwmu.RLock()
	//rf.machine.print("sending AE to %d", server)
	args := AppendEntriesArgs{
		Term:     rf.machine.currentTerm,
		LeaderId: rf.me,
	}
	rf.machine.rwmu.RUnlock()
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)

	if ok {
		rf.machine.rwmu.RLock()
		if reply.Term > rf.machine.currentTerm {
			rf.machine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
		} else {
			// TODO log state
		}
		rf.machine.rwmu.RUnlock()
	}
	cond.L.Lock()
	*joinCount++
	if *joinCount+1 >= rf.peerCount() {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

func (rf *Raft) sendAEs() {
	joinCount := 0
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < rf.peerCount(); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendSingleAE(i, &joinCount, cond)
	}
	cond.L.Lock()
	for joinCount+1 < rf.peerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}
