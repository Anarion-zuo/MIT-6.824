package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const candidateState int = 0
const leaderState int = 1
const followerState int = 2

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// Lock to protect shared access to this peer's state
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int // state of this server, Candidate, Leader, Follower

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	clearElectionTimer bool
	clearElectionMutex sync.Mutex

	// log flags
	electLogFlag bool
}

func (rf *Raft) logPrintf(format string, vars ...interface{}) {
	var stateString string
	state := rf.state
	switch state {
	case candidateState:
		stateString = "candidate"
		break
	case leaderState:
		stateString = "leader"
		break
	case followerState:
		stateString = "follower"
		break
	default:
		log.Fatal("wrong state config...")
		break
	}
	rightHalf := fmt.Sprintf(format, vars...)
	log.Printf("%d %s term %d %s", rf.me, stateString, rf.currentTerm, rightHalf)
}

func (rf *Raft) logElectPrintf(format string, vars ...interface{}) {
	if rf.electLogFlag {
		rf.logPrintf(format, vars...)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isLeader := rf.state == leaderState
	rf.mu.Unlock()
	return rf.currentTerm, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) checkLastApplied() {
	// if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	if rf.commitIndex > rf.lastApplied {
		// TODO...
	}
}

// if RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower
// returns whether the state is transferred
func (rf *Raft) checkTerm(term int, leaderId int) bool {
	if term > rf.currentTerm {
		// must transfer state
		rf.logElectPrintf("RPC hears a term greater than current, transfer to follower...\n")
		rf.state = followerState
		rf.currentTerm = term
		rf.votedFor = leaderId
		return true
	}
	return false
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.logElectPrintf("got RequestVote from server %d at term %d\n", args.CandidateId, args.Term)

	// lock on rf state
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// if rf.checkTerm(args.Term) {
	// 	return
	// }
	// this is delt by the following if
	rf.checkTerm(args.Term, args.CandidateId)

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// if votedFor is null or candidateId, and candidate's log is atleast as up-to-date as receiver's log, grant vote.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogIndex >= len(rf.log)-1) {
		rf.logElectPrintf("grant vote to %d\n", args.CandidateId)
		reply.VoteGranted = true
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func randWaitTime(randomMax int, timeUnitMs int) time.Duration {
	unit := rand.Intn(randomMax)
	return time.Duration(unit*timeUnitMs) * time.Millisecond
}

func (rf *Raft) asyncSendRequestVote() (*[]RequestVoteReply, *[]bool) {
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// index of candidate's last log entry
		LastLogIndex: len(rf.log) - 1,
		// term of candidate's last log entry
		LastLogTerm: 0,
	}
	replyArray := make([]RequestVoteReply, len(rf.peers))
	okCount := 0
	grantedCount := 0
	recvedCount := 0
	okArray := make([]bool, len(rf.peers))
	cond := sync.NewCond(&sync.Mutex{})

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// lock to prevent data race at pointers
			cond.L.Lock()
			go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, cond *sync.Cond, recvedCount *int, okCount *int, grantedCount *int) {
				rf.logElectPrintf("sending RequestVote to %d\n", server)
				ok := rf.sendRequestVote(server, args, reply)
				rf.logElectPrintf("reply from %d ok %t\n", server, ok)
				cond.L.Lock()
				if ok {
					*okCount++
				}
				if reply.VoteGranted {
					*grantedCount++
				}
				*recvedCount++
				if *grantedCount+1 > len(rf.peers)/2 {
					rf.logElectPrintf("early elected by major grantedCount %d, total %d\n", *grantedCount, len(rf.peers))
					cond.Broadcast()
				}
				if *recvedCount+1 >= len(rf.peers) {
					cond.Broadcast()
				}
				cond.L.Unlock()
			}(i, &args, &replyArray[i], cond, &recvedCount, &okCount, &grantedCount)
			cond.L.Unlock()
		}
	}

	// join all peers
	cond.L.Lock()
	for recvedCount+1 < len(rf.peers) && grantedCount+1 <= len(rf.peers)/2 {
		cond.Wait()
	}

	rf.logElectPrintf("enough votes gathered...\n")

	if grantedCount+1 > (okCount+1)/2 || grantedCount+1 > len(rf.peers)/2 {
		rf.logElectPrintf("got elected %d votes %d oks\n", grantedCount, okCount)
		rf.mu.Lock()
		if rf.state != candidateState {

		} else {
			rf.state = leaderState
		}
		rf.mu.Unlock()
	} else {

	}
	// lock until here
	// to ensure atomic access on all count variables
	cond.L.Unlock()

	return &replyArray, &okArray
}

// on conversion to candidate, start election
// after this function is done, the server would be in a state other than candidate
func (rf *Raft) doElection() {
	rf.logElectPrintf("start election\n")
	for {
		// random wait
		waitTime := randWaitTime(10, 2)
		rf.logElectPrintf("wait for %dms\n", waitTime/time.Millisecond)
		time.Sleep(waitTime)

		// lock on rf state
		rf.mu.Lock()

		// check state
		if rf.state != candidateState {
			rf.mu.Unlock()
			break
		}

		// increment current Term
		rf.currentTerm++
		// vote for self
		rf.votedFor = rf.me

		rf.mu.Unlock()

		// reset election timer
		rf.clearElectionMutex.Lock()
		rf.clearElectionTimer = true
		rf.clearElectionMutex.Unlock()
		// send RequestVote RPCs to all others
		// votedCount := 0

		rf.asyncSendRequestVote()
		// state already determined
		rf.logElectPrintf("all RequestVote sent and received...\n")
		// for i := 0; i < len(*replyArray); i++ {
		// 	if (*okArray)[i] {
		// 		//rf.checkLastApplied()
		// 		if (*replyArray)[i].VoteGranted {
		// 			rf.logPrintf("vote granted by %d\n", i)
		// 			votedCount++
		// 		} else {
		// 			rf.logPrintf("vote denied by %d\n", i)
		// 		}
		// 		// might transfer state
		// 		if rf.checkTerm((*replyArray)[i].Term) {
		// 			break
		// 		}
		// 	}
		// }

		// try state transfer
		rf.mu.Lock()
		if rf.state != candidateState {
			// already transferred
			// exit election
			rf.logElectPrintf("%d got elected\n", rf.votedFor)
			rf.mu.Unlock()
			break
		}
		rf.logElectPrintf("still a candidate\n")
		// this is the correct comparison under odd or even peer count
		// if votedCount + 1 > len(rf.peers) / 2 {
		// 	rf.state = leaderState
		// 	rf.mu.Unlock()
		// 	rf.logPrintf("I got elected by vote of majority %d votes!\n", votedCount)
		// 	break
		// }
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// lock on rf state
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// if rf.checkTerm(args.Term) {

	// }
	// this is delt by the following if
	rf.checkTerm(args.Term, args.LeaderId)

	rf.logElectPrintf("AppendEntries received from server %d at term %d\n", args.LeaderId, args.Term)

	// clears timer
	rf.clearElectionMutex.Lock()
	rf.clearElectionTimer = true
	rf.clearElectionMutex.Unlock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// TODO ...

	/**
	If an existing entry conflicts with a new one (same index
	but different terms), delete the existing entry and all
	that follow it.
	*/
	// TODO ...

	// Append any new entries not already in the log
	// TODO ...

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// TODO ...

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

const followerMaxCheck = 200
const followerWaitTimeUnitMs = 10

// follower main process
// when this exits, it can only be that this server is no longer a follower
func (rf *Raft) followerMain() {
	rf.logElectPrintf("enter follower loop\n")
	for timeUnit := 0; timeUnit < followerMaxCheck; timeUnit++ {
		time.Sleep(time.Duration(followerWaitTimeUnitMs) * time.Millisecond)
		// check for state transfer
		rf.mu.Lock()
		if rf.state != followerState {
			rf.mu.Unlock()
			rf.logElectPrintf("exit follower state\n")
			break
		}
		rf.mu.Unlock()
		rf.clearElectionMutex.Lock()
		if rf.clearElectionTimer {
			timeUnit = 0
			rf.clearElectionTimer = false
			rf.logElectPrintf("election timer cleared\n")
		} else {
			// rf.logPrintf("election timer not cleared...\n")
		}
		rf.clearElectionMutex.Unlock()
	}
	// if this ever gets here, must convert to candidate
	rf.mu.Lock()
	rf.state = candidateState
	rf.mu.Unlock()
	rf.logElectPrintf("election timeout, converted to candidate\n")
}

func (rf *Raft) asyncSendAppendEntries() (*[]AppendEntriesReply, *[]bool) {
	// lock on rf state
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.votedFor,
		// PrevLogIndex: ,
		// Entries: ,
		// LeaderCommit: ,
	}
	rf.mu.Unlock()
	replyArray := make([]AppendEntriesReply, len(rf.peers))
	okCount := 0
	grantedCount := 0
	recvedCount := 0
	okArray := make([]bool, len(rf.peers))
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, cond *sync.Cond, recvedCount *int, okCount *int, grantedCount *int) {
				// rf.logPrintf("sending AppendEntries to %d\n", server)
				ok := rf.sendAppendEntries(server, args, reply)
				// rf.logPrintf("reply from %d ok %t\n", server, ok)
				cond.L.Lock()
				if ok {
					*okCount++
				}
				*recvedCount++
				if *recvedCount+1 >= len(rf.peers) {
					cond.Broadcast()
				}
				cond.L.Unlock()
			}(i, &args, &replyArray[i], cond, &recvedCount, &okCount, &grantedCount)
		}
	}
	// do not have to join anyone
	return &replyArray, &okArray
}

/**
The routine sending out heartbeat signal for a leader.
The signals are sent in roughly identical pace, without
any random wait.
*/
const heartBeatWaitMs = 110

// this returns only when this server is no longer a leader
func (rf *Raft) leaderMain() {
	// upon election:
	// send initial empty AppendEntries RPCs to each server
	// repeat during idle periods to prevent election timeouts
	for {
		// wait for constant time then send
		// rf.logPrintf("waiting %dms to send AppendEntries\n", heartBeatWaitMs)
		time.Sleep(time.Duration(heartBeatWaitMs) * time.Millisecond)
		rf.asyncSendAppendEntries()
		// if command received from clien:...
		// TODO...

		// if last log index >= nextIndex for a follower...
		// TODO...

		// if there exists an N such that N > commitIndex...
		// TODO...

		// possible state transfer
		rf.mu.Lock()
		if rf.state != leaderState {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) mainLoop() {
	for !rf.killed() {
		// atomically read state
		rf.mu.Lock()
		curState := rf.state
		rf.mu.Unlock()
		switch curState {
		case leaderState:
			rf.leaderMain()
			break
		case candidateState:
			rf.doElection()
			break
		case followerState:
			rf.followerMain()
			break
		default:
			log.Fatalln("undefined raft state???")
			break
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// reinitialized after election
	rf.nextIndex = make([]int, len(peers))
	// initialized to leader's last log index + 1

	rf.matchIndex = make([]int, len(peers))
	// initialized to 0
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	// set to true if want to clear timer
	// follower loop checks and clears this
	rf.clearElectionTimer = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// init seed at make time
	rand.Seed(time.Now().UnixNano())

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.mainLoop()

	return rf
}
