package raft

import (
	"sync"
)

type ElectionTimeout struct {
	RaftStateTransfer
}

func (rf *Raft) makeElectionTimeout() *ElectionTimeout {
	return &ElectionTimeout{
		RaftStateTransfer{
			machine: rf.stateMachine,
		},
	}
}

func (trans *ElectionTimeout) transfer(source SMState) SMState {
	// check state
	// can only transfer from follower or candidate state
	if source != followerState && source != startElectionState {
		trans.machine.raft.print("not transferred from follower or candidate")
		return notTransferred
	}
	trans.machine.raft.print("begin election")
	trans.machine.raft.electionTimer.stop()
	trans.machine.raft.sendAETimer.stop()
	// On conversion to candidate, start election
	// Increment currentTerm
	trans.machine.currentTerm++
	// Vote for self
	trans.machine.votedFor = trans.machine.raft.me
	// Reset election timer
	trans.machine.raft.electionTimer.setElectionWait()
	trans.machine.raft.electionTimer.start()

	// Send RequestVote RPCs to all other servers
	go trans.machine.raft.doElect()
	trans.machine.raft.persist()
	return startElectionState
}

func (trans *ElectionTimeout) getName() string {
	return "ElectionTimeout"
}

func (trans *ElectionTimeout) isRW() bool {
	return true
}

func (rf *Raft) sendJoinRequestVote(server int, voteCount *int, joinCount *int, elected *bool, cond *sync.Cond) {
	rf.stateMachine.rwmu.RLock()
	args := RequestVoteArgs{
		Term:        rf.stateMachine.currentTerm,
		CandidateId: rf.me,
		// TODO log fields
	}
	reply := RequestVoteReply{}
	rf.stateMachine.raft.print("sending RequestVote to %d", server)
	args.LastLogIndex = rf.stateMachine.lastLogIndex()
	args.LastLogTerm = rf.stateMachine.getEntry(args.LastLogIndex).Term
	rf.stateMachine.rwmu.RUnlock()

	ok := rf.sendRequestVote(server, &args, &reply)

	rf.stateMachine.rwmu.RLock()
	if ok {
		if reply.Term > rf.stateMachine.currentTerm {
			rf.stateMachine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
		} else {
			rf.stateMachine.raft.print("server %d reply ok %t grant %t", server, ok, reply.VoteGranted)
			if reply.VoteGranted {
				cond.L.Lock()
				*voteCount++
				cond.L.Unlock()
			}
			// If votes received from majority of servers: become leader
			cond.L.Lock()
			if *voteCount+1 > rf.PeerCount()/2 {
				if !*elected {
					rf.stateMachine.raft.print("got elected on %d votes from %d peers", *voteCount, rf.PeerCount())
					rf.stateMachine.issueTransfer(rf.makeMajorElected())
					*elected = true
				}
			}
			cond.L.Unlock()
		}
	} else {
		rf.stateMachine.raft.print("server %d unreachable", server)
	}
	rf.stateMachine.rwmu.RUnlock()
	cond.L.Lock()
	*joinCount++
	if *joinCount+1 >= rf.PeerCount() {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

func (rf *Raft) doElect() {
	voteCount := 0
	joinCount := 0
	elected := false
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < rf.PeerCount(); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendJoinRequestVote(i, &voteCount, &joinCount, &elected, cond)
	}
	cond.L.Lock()
	for joinCount+1 < rf.PeerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}
