package raft

import "sync"

//const candidateState int = 0
//const leaderState int = 1
//const followerState int = 2

type RaftState struct {
	state       int // state of this server, Candidate, Leader, Follower
	currentTerm int
	votedFor    int

	rwmutex sync.RWMutex
}

func (rs *RaftState) getState() int {
	return rs.state
}

func (rs *RaftState) isState(state int) bool {
	return rs.state == state
}

func (rs *RaftState) rLock() {
	rs.rwmutex.RLock()
}

func (rs *RaftState) rUnlock() {
	rs.rwmutex.RUnlock()
}

func (rs *RaftState) wLock() {
	rs.rwmutex.Lock()
}

func (rs *RaftState) wUnlock() {
	rs.rwmutex.Unlock()
}

/**
*********************************
	Candidate state operations
*********************************
*/

/**
 * Before the election starts,
 */
func (rs *RaftState) beforeSendRequestVote(me int) {
	// increment current Term
	rs.currentTerm++
	// vote for self
	rs.votedFor = me
}

// if RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower
// returns whether the state is transferred
func (rs *RaftState) checkTerm(term int, leaderId int) bool {
	if term > rs.currentTerm {
		// must transfer state
		rs.state = followerState
		rs.currentTerm = term
		rs.votedFor = leaderId
		return true
	}
	return false
}

/**
 * @returns whether should grant vote based on raft state information
 */
func (rs *RaftState) requestVoteStateProcess(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	reply.Term = rs.currentTerm
	if rs.checkTerm(args.Term, args.CandidateId) {
		return true
	}
	// reply false if term < currentTerm
	if args.Term < rs.currentTerm {
		return false
	}
	// if votedFor is null or candidateId, and candidate's log is atleast as up-to-date as receiver's log, grant vote.
	if rs.votedFor == -1 || rs.votedFor == args.CandidateId {
		rs.votedFor = args.CandidateId
		return true
	}
	return false
}

/**
*********************************
	Candidate state end
*********************************
*/

/**
*********************************
	AppendEntries begin
*********************************
*/

/**
 * Sets reply term
 * @returns whether Success based on raft state information
 */ /*
func (rs *RaftState) apendEntriesStateProcess(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	reply.Term = rs.currentTerm
	rs.checkTerm(args.Term, args.LeaderId)
	// reply false if term < currentTerm
	if args.Term < rs.currentTerm {
		return false
	}
	return true
}*/

/**
*********************************
	AppendEntries end
*********************************
*/

func MakeRaftState() *RaftState {
	rs := &RaftState{
		currentTerm: 0,
		votedFor:    -1,
		state:       followerState,
	}
	return rs
}
