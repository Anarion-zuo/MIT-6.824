package raft

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
		trans.machine.raft.print("transfer to leader after successful election")
		trans.machine.raft.stateMachine.initVolatile(trans.machine.raft.PeerCount())
	} else {
		trans.machine.raft.print("AE timer timeout")
	}
	trans.machine.raft.electionTimer.stop()
	// start send AE
	trans.machine.raft.sendAEs()
	// set timer
	trans.machine.raft.sendAETimer.start()

	trans.machine.raft.persist()

	return sendAEState
}

func (rf *Raft) initAEArgsLog(server int, args *AppendEntriesArgs) {
	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	nextIndex := rf.stateMachine.nextIndex[server]
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.stateMachine.getEntry(args.PrevLogIndex).Term
	args.LeaderCommit = rf.stateMachine.commitIndex
	if rf.stateMachine.lastLogIndex() >= nextIndex {
		args.Entries = rf.stateMachine.log[rf.stateMachine.physicalIndex(nextIndex):]
	} else {
		args.Entries = nil
	}
}

func (rf *Raft) sendAEorIS(server int) {
	if rf.stateMachine.nextIndex[server]-1 < rf.stateMachine.lastSnapshotIndex {
		rf.sendSingleIS(server, rf.persister.ReadSnapshot())
	} else {
		rf.sendSingleAE(server)
	}
}

func (rf *Raft) sendSingleAE(server int) {
	//rf.stateMachine.rwmu.RLock()
	args := AppendEntriesArgs{
		Term:     rf.stateMachine.currentTerm,
		LeaderId: rf.me,
	}
	rf.initAEArgsLog(server, &args)
	//rf.print("sending to server %d %d entries", server, len(args.Entries))
	//rf.stateMachine.rwmu.RUnlock()
	reply := AppendEntriesReply{}

	go func() {
		ok := rf.sendAppendEntries(server, &args, &reply)

		if ok {
			rf.stateMachine.rwmu.Lock()
			rf.print("reply from follower %d success %t", server, reply.Success)
			if reply.Term > rf.stateMachine.currentTerm {
				rf.stateMachine.callTransfer(rf.makeLargerTerm(reply.Term, server))
			} else {
				rf.stateMachine.callTransfer(rf.makeAEReplied(server, &args, &reply))
			}
			rf.stateMachine.rwmu.Unlock()
		}
	}()
}

func (rf *Raft) sendAEs() {
	//rf.stateMachine.rwmu.RLock()
	//nexts := make([]int, len(rf.stateMachine.nextIndex))
	//copy(nexts, rf.stateMachine.nextIndex)
	//lastSnapshotIndex := rf.stateMachine.lastSnapshotIndex
	//rf.stateMachine.rwmu.RUnlock()
	for i := 0; i < rf.PeerCount(); i++ {
		if i == rf.me {
			continue
		}
		rf.sendAEorIS(i)
	}
	//cond.L.Lock()
	//for joinCount+1 < rf.PeerCount() {
	//	cond.Wait()
	//}
	//cond.L.Unlock()
}
