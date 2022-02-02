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
	if source == startElectionState {
		// init volatile
		trans.machine.raft.print("first sendAE after elected")
		trans.machine.raft.log.initVolatile(trans.machine.raft.peerCount())
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
	nextIndex := rf.log.nextIndex[server]
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.log.getEntry(args.PrevLogIndex).Term
	args.LeaderCommit = rf.log.commitIndex
	if rf.log.lastLogIndex() >= nextIndex {
		args.Entries = rf.log.log[nextIndex:]
	} else {
		args.Entries = nil
	}
}

func (rf *Raft) sendSingleAE(server int, joinCount *int, cond *sync.Cond) {
	rf.machine.rwmu.RLock()
	//rf.machine.print("sending AE to %d", server)
	args := AppendEntriesArgs{
		Term:     rf.machine.currentTerm,
		LeaderId: rf.me,
	}
	rf.machine.rwmu.RUnlock()
	rf.log.rwmu.RLock()
	rf.initAEArgsLog(server, &args)
	rf.log.rwmu.RUnlock()
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)

	if ok {
		rf.log.rwmu.RLock()
		rf.machine.rwmu.RLock()
		rf.print("reply from follower %d success %t", server, reply.Success)
		if reply.Term > rf.machine.currentTerm {
			rf.machine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
		} else {
			rf.log.issueTransfer(rf.makeAEReplied(server, len(args.Entries), reply.Success))
		}
		rf.log.rwmu.RUnlock()
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
