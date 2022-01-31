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
			machine: rf.machine,
		},
	}
}

func (trans *ElectionTimeout) transfer(source SMState) SMState {
	// check state
	// can only transfer from follower or candidate state
	if source != followerState && source != startElectionState {
		trans.machine.print("not transferred from follower or candidate")
		return notTransferred
	}
	trans.machine.print("begin election")
	trans.machine.raft.electionTimer.stop()
	// random wait to get rid of tickling deadlock
	//randWaitMs := rand.Int() % 100
	//trans.machine.print("random wait for %dms", randWaitMs)
	//time.Sleep(time.Duration(randWaitMs) * time.Millisecond)
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
	return startElectionState
}

func (trans *ElectionTimeout) getName() string {
	return "ElectionTimeout"
}

func (rf *Raft) sendJoinRequestVote(server int, voteCount *int, joinCount *int, elected *bool, cond *sync.Cond) {
	rf.machine.rwmu.RLock()
	args := RequestVoteArgs{
		Term:        rf.machine.currentTerm,
		CandidateId: rf.me,
		// TODO log fields
	}
	reply := RequestVoteReply{}
	rf.machine.print("sending RequestVote to %d", server)
	rf.machine.rwmu.RUnlock()

	ok := rf.sendRequestVote(server, &args, &reply)

	cond.L.Lock()
	rf.machine.rwmu.RLock()
	if ok {
		if reply.Term > rf.machine.currentTerm {
			rf.machine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
		} else {
			rf.machine.print("server %d reply ok %t grant %t", server, ok, reply.VoteGranted)
			if reply.VoteGranted {
				*voteCount++
			}
			// If votes received from majority of servers: become leader
			if *voteCount+1 > rf.peerCount()/2 {
				rf.machine.print("got elected on %d votes from %d peers", *voteCount, rf.peerCount())
				if !*elected {
					rf.machine.issueTransfer(rf.makeMajorElected())
					*elected = true
				}
			}
		}
	} else {
		rf.machine.print("server %d unreachable", server)
	}
	rf.machine.rwmu.RUnlock()
	*joinCount++
	if *joinCount+1 >= rf.peerCount() {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

func (rf *Raft) doElect() {
	voteCount := 0
	joinCount := 0
	elected := false
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < rf.peerCount(); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendJoinRequestVote(i, &voteCount, &joinCount, &elected, cond)
	}
	cond.L.Lock()
	for joinCount+1 < rf.peerCount() {
		cond.Wait()
	}
	cond.L.Unlock()
}
