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
	return &MajorElected{RaftStateTransfer{machine: rf.stateMachine}}
}

func (trans *MajorElected) transfer(source SMState) SMState {
	// check state
	if source != startElectionState && source != sendAEState {
		return notTransferred
	}
	if source == startElectionState {
		// init volatile
		trans.machine.raft.print("first sendAE after elected")
		trans.machine.raft.logMachine.initVolatile(trans.machine.raft.peerCount())
	}
	trans.machine.raft.electionTimer.stop()
	// start send AE
	go trans.machine.raft.sendAEs()
	// set timer
	trans.machine.raft.sendAETimer.start()

	return sendAEState
}

func (rf *Raft) initAEArgsLog(server int, args *AppendEntriesArgs) {
	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	nextIndex := rf.logMachine.nextIndex[server]
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.logMachine.getEntry(args.PrevLogIndex).Term
	args.LeaderCommit = rf.logMachine.commitIndex
	if rf.logMachine.lastLogIndex() >= nextIndex {
		args.Entries = rf.logMachine.log[nextIndex:]
	} else {
		args.Entries = nil
	}
}

func (rf *Raft) sendSingleAE(server int, joinCount *int, cond *sync.Cond) {
	rf.stateMachine.rwmu.RLock()
	//rf.stateMachine.print("sending AE to %d", server)
	args := AppendEntriesArgs{
		Term:     rf.stateMachine.currentTerm,
		LeaderId: rf.me,
	}
	rf.stateMachine.rwmu.RUnlock()
	rf.logMachine.rwmu.RLock()
	rf.initAEArgsLog(server, &args)
	rf.logMachine.rwmu.RUnlock()
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)

	if ok {
		rf.logMachine.rwmu.RLock()
		rf.stateMachine.rwmu.RLock()
		rf.print("reply from follower %d success %t", server, reply.Success)
		if reply.Term > rf.stateMachine.currentTerm {
			rf.stateMachine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
		} else {
			rf.logMachine.issueTransfer(rf.makeAEReplied(server, len(args.Entries), reply.Success, reply.ConflictPrevIndex, reply.ConflictPrevTerm))
		}
		rf.logMachine.rwmu.RUnlock()
		rf.stateMachine.rwmu.RUnlock()
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
