package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
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
	Command interface{}
	Term    int
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

	raftState *RaftState

	log []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	//logRw                 sync.RWMutex
	logRw     deadlock.RWMutex
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	//applyCond             *deadlock.Cond
	lastAppendEntriesTime time.Time

	clearElectionTimer bool
	//clearElectionMutex sync.Mutex
	clearElectionMutex deadlock.Mutex

	// log flags
	electLogFlag bool
	logLogFlag   bool
}

//func (rf *Raft) tryApply() {
//	for rf.lastApplied < rf.commitIndex {
//		rf.lastApplied++
//		rf.raftState.rLock()
//		rf.logLogPrintf("applying entry %d\n", rf.lastApplied)
//		rf.raftState.rUnlock()
//		rf.applyCh <- ApplyMsg{
//			Command:      rf.log[rf.lastApplied].Command,
//			CommandValid: true,
//			CommandIndex: rf.lastApplied,
//		}
//	}
//}

//const tryApplyWaitMs int = 10

func (rf *Raft) tryApplyLoop() {
	for {
		rf.applyCond.L.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		nextApplied := rf.lastApplied + 1
		toBeApplied := rf.log[nextApplied : rf.commitIndex+1]
		// send to channel
		go func(nextApplied int, toBeApplied *[]LogEntry) {
			for i, entry := range *toBeApplied {
				rf.applyCh <- ApplyMsg{
					Command:      entry.Command,
					CommandValid: true,
					CommandIndex: i + nextApplied,
				}
			}
		}(nextApplied, &toBeApplied)
		rf.raftState.rLock()
		rf.logLogPrintf("applied %d entries %v\n", len(toBeApplied), toBeApplied)
		rf.lastApplied += len(toBeApplied)
		//for i, entry := range toBeApplied {
		//	// must pass variables as values
		//	go func(entry LogEntry, i int, nextApplied int) {
		//		rf.applyCh <- ApplyMsg{
		//			Command:      entry.Command,
		//			CommandValid: true,
		//			CommandIndex: i + nextApplied,
		//		}
		//	}(entry, i, nextApplied)
		//}
		rf.raftState.rUnlock()
		rf.applyCond.L.Unlock()
	}
}

func (rf *Raft) logPrintf(format string, vars ...interface{}) {
	var stateString string
	state := rf.raftState.state
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
	log.Printf("%d %s term %d votedFor %d lastLogIndex %d commit %d lastApplied %d | %s", rf.me, stateString, rf.raftState.currentTerm, rf.raftState.votedFor, rf.lastLogIndex(), rf.commitIndex, rf.lastApplied, rightHalf)
}

func (rf *Raft) logElectPrintf(format string, vars ...interface{}) {
	if rf.electLogFlag {
		rf.logPrintf(format, vars...)
	}
}

func (rf *Raft) logLogPrintf(format string, vars ...interface{}) {
	if rf.logLogFlag {
		rf.logPrintf(format, vars...)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var Term int
	// var isleader bool
	// Your code here (2A).
	rf.raftState.rLock()
	isLeader := rf.raftState.state == leaderState
	currentTerm := rf.raftState.currentTerm
	//rf.logElectPrintf("asked state, answer term %d isLeader %t\n", currentTerm, isLeader)
	rf.raftState.rUnlock()
	return currentTerm, isLeader
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
	rf.raftState.wLock()

	rf.logElectPrintf("got RequestVote from server %d at Term %d\n", args.CandidateId, args.Term)
	reply.VoteGranted = false

	reply.Term = rf.raftState.currentTerm

	stateBool := rf.raftState.requestVoteStateProcess(args, reply)

	// proceed only when args.Term == rf.currentTerm

	rf.raftState.wUnlock()

	rf.logRw.RLock()
	rf.raftState.rLock()
	var logBool bool
	myLastLogTerm := rf.log[rf.lastLogIndex()].Term
	if myLastLogTerm != args.LastLogTerm {
		/**
		If the logs have last entries with different terms, then
		the log with the later term is more up-to-date.
		*/
		logBool = args.LastLogTerm >= myLastLogTerm
	} else {
		/**
		If the logs end with the same term, then whichever log
		is longer is more up-to-date.
		*/
		logBool = args.LastLogIndex >= rf.lastLogIndex()
	}
	if !logBool {
		rf.logElectPrintf("reject vote because candidate's log at index %d term %d is not as up-to-date as mine index %d term %d\n", args.LastLogIndex, args.Term, rf.lastLogIndex(), rf.log[rf.lastLogIndex()].Term)
	} else {
		rf.logElectPrintf("reject vote because candidate's log at index %d term %d is as up-to-date as mine index %d term %d\n", args.LastLogIndex, args.Term, rf.lastLogIndex(), rf.log[rf.lastLogIndex()].Term)
	}
	rf.raftState.rUnlock()
	rf.logRw.RUnlock()

	// if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	if stateBool && logBool {
		rf.raftState.rLock()
		rf.logElectPrintf("grant vote to %d\n", args.CandidateId)
		rf.raftState.rUnlock()
		reply.VoteGranted = true
	}

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

func (rf *Raft) asyncSendRequestVote() {
	rf.logRw.RLock()
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.log[rf.lastLogIndex()].Term
	rf.logRw.RUnlock()
	rf.raftState.rLock()
	if !rf.raftState.isState(candidateState) {
		rf.raftState.rUnlock()
		return
	}
	args := RequestVoteArgs{
		Term:        rf.raftState.currentTerm,
		CandidateId: rf.me,
		// index of candidate's last log entry
		LastLogIndex: lastLogIndex,
		// Term of candidate's last log entry
		LastLogTerm: lastLogTerm,
	}
	rf.raftState.rUnlock()
	okCount := 0
	grantedCount := 0
	recvedCount := 0
	cond := sync.NewCond(&sync.Mutex{})

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// lock to prevent data race at pointers
			cond.L.Lock()
			go func(server int, args *RequestVoteArgs, cond *sync.Cond, recvedCount *int, okCount *int, grantedCount *int) {
				rf.raftState.rLock()
				rf.logElectPrintf("sending RequestVote to %d\n", server)
				rf.raftState.rUnlock()
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)
				rf.raftState.rLock()
				rf.logElectPrintf("reply from %d ok %t granted %t\n", server, ok, reply.VoteGranted)
				rf.raftState.rUnlock()
				cond.L.Lock()
				if ok {
					*okCount++
					rf.raftState.wLock()
					if rf.raftState.checkTerm(reply.Term, server) {
						rf.raftState.wUnlock()
						cond.Broadcast()
						cond.L.Unlock()
						return
					}
					rf.raftState.wUnlock()
				}
				if reply.VoteGranted {
					*grantedCount++
				}
				*recvedCount++
				if *grantedCount+1 > len(rf.peers)/2 {
					rf.logRw.RLock()
					rf.raftState.rLock()
					rf.logElectPrintf("early elected by major grantedCount %d, total %d\n", *grantedCount, len(rf.peers))
					rf.logRw.RUnlock()
					rf.raftState.rUnlock()
					cond.Broadcast()
				}
				if *recvedCount+1 >= len(rf.peers) {
					cond.Broadcast()
				}
				cond.L.Unlock()
			}(i, &args, cond, &recvedCount, &okCount, &grantedCount)
			cond.L.Unlock()
		}
	}

	// join all peers
	cond.L.Lock()
	for recvedCount+1 < len(rf.peers) && grantedCount+1 <= len(rf.peers)/2 {
		cond.Wait()
	}

	rf.logElectPrintf("enough servers replied...\n")

	// do not make oneself leader when it is connected to no one
	//if (grantedCount+1 > (okCount+1)/2) || grantedCount+1 > len(rf.peers)/2 {
	//if (grantedCount+1 > (okCount+1)/2 && okCount != 0) || grantedCount+1 > len(rf.peers)/2 {
	if grantedCount+1 > len(rf.peers)/2 {
		rf.logElectPrintf("got elected %d votes %d oks\n", grantedCount, okCount)
		rf.raftState.wLock()
		if !rf.raftState.isState(candidateState) {
			rf.raftState.wUnlock()
			cond.L.Unlock()
			return
		}
		rf.raftState.state = leaderState
		rf.raftState.wUnlock()
	} else {

	}
	// lock until here
	// to ensure atomic access on all count variables
	cond.L.Unlock()

	return
}

// on conversion to candidate, start election
// after this function is done, the server would be in a state other than candidate
func (rf *Raft) doElection() {
	rf.logElectPrintf("start election\n")
	for {
		// random wait
		/**
		This wait time must be large enough.
		After each round of sending RequestVote, AppendEntries must be allowed to get in.
		Or else the candidate keeps issueing an election, making its own Term huge,
		while others acheived consensus and ignores it.

		This is unnecessary because the hosts getting a Term larger than its own must submit.
		*/
		waitTime := randWaitTime(500, 1)
		rf.logRw.RLock()
		rf.raftState.rLock()
		rf.logElectPrintf("wait for %dms\n", waitTime/time.Millisecond)
		rf.raftState.rUnlock()
		rf.logRw.RUnlock()
		time.Sleep(waitTime)

		// lock on rf state
		//rf.mu.Lock()
		rf.raftState.wLock()

		// check state
		if rf.raftState.state != candidateState {
			//rf.mu.Unlock()
			rf.raftState.wUnlock()
			break
		}

		rf.raftState.beforeSendRequestVote(rf.me)

		rf.raftState.wUnlock()

		// reset election timer
		rf.clearElectionMutex.Lock()
		rf.clearElectionTimer = true
		rf.clearElectionMutex.Unlock()

		rf.asyncSendRequestVote()
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
	Term         int
	Success      bool
	ConflictTerm int
	// the first index of the log entry
	// having less term than the conflicting term
	ConflictTermPrevIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false // default value

	// lock on rf state
	rf.logRw.Lock()
	rf.raftState.wLock()
	rf.logLogPrintf("AppendEntries recved from leader %d with %d entries at commit %d\n", args.LeaderId, len(args.Entries), args.LeaderCommit)

	reply.Term = rf.raftState.currentTerm

	if rf.raftState.checkTerm(args.Term, args.LeaderId) {
		rf.logElectPrintf("encounters a greater Term %d in AppendEntries from server %d\n", args.Term, args.LeaderId)
		//rf.raftState.wUnlock()
		//return
	}

	// reply false if Term < currentTerm
	if args.Term < rf.raftState.currentTerm {
		rf.logLogPrintf("leader term %d less than my term %d\n", args.Term, rf.raftState.currentTerm)
		rf.raftState.wUnlock()
		rf.logRw.Unlock()
		return
	}

	// clears timer
	rf.clearElectionMutex.Lock()
	rf.clearElectionTimer = true
	rf.clearElectionMutex.Unlock()

	// reply false if log doesn't contain an entry at prevLogIndex whose Term matches prevLogTerm
	// TODO ...
	if args.PrevLogIndex >= len(rf.log) {
		rf.logLogPrintf("prevLogIndex %d ge than length of log %d\n", args.PrevLogIndex, len(rf.log))
		rf.logRw.Unlock()
		rf.raftState.wUnlock()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logLogPrintf("log at prevLogIndex Term %d not matched to leader %d prevLogTerm %d\n", rf.log[args.PrevLogIndex].Term, args.LeaderId, args.PrevLogTerm)

		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictTermPrevIndex = i
			}
		}

		rf.logRw.Unlock()
		rf.raftState.wUnlock()
		return
	}
	rf.logLogPrintf("log term %d matches with leader %d at prevLogIndex %d\n", rf.log[args.PrevLogIndex].Term, args.LeaderId, args.PrevLogTerm)
	reply.Success = true

	/**
	If an existing entry conflicts with a new one (same index
	but different terms), delete the existing entry and all
	that follow it.
	*/
	// TODO ...
	rf.log = rf.log[:args.PrevLogIndex+1]

	// Append any new entries not already in the log
	// TODO ...
	rf.log = append(rf.log, args.Entries...)
	rf.logLogPrintf("appended %d entries from leader\n", len(args.Entries))

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// TODO ...
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastLogIndex() < args.LeaderCommit {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.logLogPrintf("commitIndex updated to %d\n", rf.commitIndex)
		rf.applyCond.Broadcast()
	}

	rf.logRw.Unlock()
	rf.raftState.wUnlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

const followerMaxCheck = 30
const followerWaitTimeUnitMs = 10

// follower main process
// when this exits, it can only be that this server is no longer a follower
func (rf *Raft) followerMain() {
	rf.logElectPrintf("enter follower loop\n")
	for timeUnit := 0; timeUnit < followerMaxCheck; timeUnit++ {
		time.Sleep(time.Duration(followerWaitTimeUnitMs) * time.Millisecond)
		// check for state transfer
		rf.raftState.rLock()
		if rf.raftState.state != followerState {
			rf.raftState.rUnlock()
			rf.logElectPrintf("exit follower state\n")
			break
		}
		rf.raftState.rUnlock()
		rf.clearElectionMutex.Lock()
		if rf.clearElectionTimer {
			timeUnit = 0
			rf.clearElectionTimer = false
			//rf.logElectPrintf("election timer cleared\n")
		} else {
			// rf.logPrintf("election timer not cleared...\n")
		}
		rf.clearElectionMutex.Unlock()
	}
	// if this ever gets here, must convert to candidate
	rf.raftState.wLock()
	if rf.raftState.isState(followerState) {
		rf.raftState.state = candidateState
		rf.logElectPrintf("election timeout, converted to candidate\n")
	}
	rf.raftState.wUnlock()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

/**
 * @return prevLogIndex, entries, leaderCommit
 */
func (rf *Raft) produceLogEntriesForFollower(server int) (int, []LogEntry, int) {
	/**
	If Command received from client: append entry to local log,
	respond after entry applied to state machine
	*/
	/**
	If last log index ≥ nextIndex for a follower:
	*/
	prevLogIndex := rf.nextIndex[server] - 1
	commitIndex := rf.commitIndex
	var entries []LogEntry = nil
	if rf.lastLogIndex() >= rf.nextIndex[server] {
		// send AppendEntries RPC with log entries starting at nextIndex
		entries = rf.log[rf.nextIndex[server]:]
		rf.logLogPrintf("server %d last log index %d ge than nextIndex %d, sending %d entries\n", server, rf.lastLogIndex(), rf.nextIndex[server], len(entries))
	}
	return prevLogIndex, entries, commitIndex
}

func (rf *Raft) tryIncrementCommitIndex() {
	/**
	If there exists an N such that N > commitIndex, a majority
	of matchIndex[i] ≥ N, and log[N].Term == currentTerm:
	set commitIndex = N
	*/
	rf.logLogPrintf("try to increment commitIndex\n")
	N := rf.commitIndex
	tempN := N + 1
	for {
		if tempN > rf.lastLogIndex() {
			break
		}
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= tempN && rf.log[tempN].Term == rf.raftState.currentTerm {
				count++
			}
		}
		if count+1 > len(rf.peers)/2 {
			N = tempN
			rf.logLogPrintf("agree count %d total count %d majority agrees on index %d\n", count, len(rf.peers), tempN)
		} else {
			rf.logLogPrintf("agree count %d total count %d majority not agrees on index %d\n", count, len(rf.peers), tempN)
		}
		tempN++
	}
	rf.commitIndex = N
	rf.applyCond.Broadcast()
	rf.logLogPrintf("refreshed commitIndex")
}

//const tryCommitWaitMs int = 20

//func (rf *Raft) tryCommitLoop() {
//	for {
//		time.Sleep(time.Duration(tryCommitWaitMs) * time.Millisecond)
//		rf.logRw.Lock()
//		rf.tryIncrementCommitIndex()
//		rf.logRw.Unlock()
//	}
//}

func (rf *Raft) printSentRecved(sentArray *[]bool, recvedArray *[]bool) {
	sentServer := make([]int, 0)
	recvedServer := make([]int, 0)
	for server, sent := range *sentArray {
		if !sent {
			sentServer = append(sentServer, server)
		}
	}
	for server, sent := range *recvedArray {
		if !sent {
			recvedServer = append(recvedServer, server)
		}
	}
	rf.logLogPrintf("followers unsent %v unrecved %v\n", sentServer, recvedServer)
}

func (rf *Raft) computeMatchIndex() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = rf.nextIndex[i] - 1
	}
}

func (rf *Raft) asyncSendAppendEntries() {

	now := time.Now()
	duration := time.Since(rf.lastAppendEntriesTime)
	rf.lastAppendEntriesTime = now

	//recvedCount := 0
	//largerTerm := false
	//cond := sync.NewCond(&sync.Mutex{})
	okArray := make([]bool, len(rf.peers))
	for i := 0; i < len(okArray); i++ {
		okArray[i] = false
	}
	sentArray := make([]bool, len(rf.peers))
	recvedArray := make([]bool, len(rf.peers))
	rpcStateMu := deadlock.Mutex{}
	rf.logRw.RLock()
	rf.raftState.rLock()
	rf.logLogPrintf("since last time %v nextIndex %v\n", duration, rf.nextIndex)
	rf.logRw.RUnlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if !rf.raftState.isState(leaderState) {
			rf.raftState.rUnlock()
			return
		}
		go func(server int, sentArray *[]bool, recvedArray *[]bool, rpcStateMu *deadlock.Mutex) {
			rf.logRw.RLock()
			rf.raftState.rLock()
			prevLogIndex, entries, leaderCommit := rf.produceLogEntriesForFollower(server)
			args := &AppendEntriesArgs{
				Term:         rf.raftState.currentTerm,
				LeaderId:     rf.raftState.votedFor,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntriesReply{}
			rf.logLogPrintf("to follower %d %d entries\n", server, len(args.Entries))
			rf.raftState.rUnlock()
			rf.logRw.RUnlock()
			//cond.L.Lock()
			rpcStateMu.Lock()
			(*sentArray)[server] = true
			rpcStateMu.Unlock()
			//cond.L.Unlock()

			ok := rf.sendAppendEntries(server, args, reply)

			rf.logRw.Lock()
			rf.raftState.wLock()
			rf.logLogPrintf("follower %d replies ok %t success %t\n", server, ok, reply.Success)
			//cond.L.Lock()
			// do not process if I am not leader
			if !rf.raftState.isState(leaderState) {
				rf.logRw.Unlock()
				rf.raftState.wUnlock()
				//*largerTerm = true
				//cond.Broadcast()
				//cond.L.Unlock()
				return
			}
			if ok {
				if rf.raftState.checkTerm(reply.Term, server) {
					rf.logRw.Unlock()
					rf.raftState.wUnlock()
					//*largerTerm = true
					//cond.Broadcast()
					//cond.L.Unlock()
					return
				}
				/**
				If successful: update nextIndex and matchIndex for follower
				*/
				if reply.Success {
					rf.logLogPrintf("follower %d accepts at nextIndex %d increment %d\n", server, rf.nextIndex[server], len(args.Entries))
					rf.nextIndex[server] += len(args.Entries)
					//rf.matchIndex[server] = rf.nextIndex[server] - 1
					rf.computeMatchIndex()
					rf.logLogPrintf("nextIndex %v matchIndex %v\n", rf.nextIndex, rf.matchIndex)
					//(*okArray)[server] = true
					rf.tryIncrementCommitIndex()
				} else {
					/**
					If AppendEntries fails because of log inconsistency:
					decrement nextIndex and retry
					*/
					//rf.nextIndex[server]--
					rf.nextIndex[server] = reply.ConflictTermPrevIndex + 1
					rf.logLogPrintf("follower %d rejects nextIndex, try %d\n", server, rf.nextIndex[server])
				}
			} else {
				rf.logLogPrintf("follower %d unreachable, try again next hearbeat cycle\n", server)
			}
			//*recvedCount++
			(*recvedArray)[server] = true
			//rf.logElectPrintf("to follower %d cycle ended, %d left\n", server, len(rf.peers)-1-*recvedCount)
			rpcStateMu.Lock()
			rf.printSentRecved(sentArray, recvedArray)
			rpcStateMu.Unlock()
			//if *recvedCount+1 >= len(rf.peers) {
			//cond.Broadcast()
			//rf.logElectPrintf("to all followers cycle ended\n")
			//}
			rf.logRw.Unlock()
			rf.raftState.wUnlock()
			//cond.L.Unlock()
		}(i, &sentArray, &recvedArray, &rpcStateMu)
	}
	rf.raftState.rUnlock()

	//cond.L.Lock()
	//for recvedCount+1 < len(rf.peers) && !largerTerm {
	//	cond.Wait()
	//}
	//cond.L.Unlock()
	//rf.raftState.rLock()
	//rf.logLogPrintf("all AppendEntries sent and acked\n")
	//if !rf.raftState.isState(leaderState) {
	//	rf.raftState.rUnlock()
	//	return
	//}
	//rf.raftState.rUnlock()
}

/**
The routine sending out heartbeat signal for a leader.
The signals are sent in roughly identical pace, without
any random wait.
*/
const heartBeatWaitMs = 100

// this returns only when this server is no longer a leader
func (rf *Raft) leaderMain() {
	// upon election:
	// send initial empty AppendEntries RPCs to each server
	// repeat during idle periods to prevent election timeouts
	rf.logRw.Lock()
	/**
	for each server, index of the next log entry
	to send to that server (initialized to leader
	last log index + 1)
	*/
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	/**
	for each server, index of highest log entry
	known to be replicated on server
	(initialized to 0, increases monotonically)
	*/
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
	rf.logRw.Unlock()
	for {
		// wait for constant time then send
		time.Sleep(time.Duration(heartBeatWaitMs) * time.Millisecond)
		rf.asyncSendAppendEntries()

		// possible state transfer
		rf.raftState.rLock()
		if rf.raftState.state != leaderState {
			rf.logElectPrintf("must exit leader state\n")
			rf.raftState.rUnlock()
			break
		}
		rf.raftState.rUnlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.logRw.Lock()
	rf.raftState.rLock()
	isLeader := rf.raftState.isState(leaderState)
	index := -1
	term := -1
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{
			Term:    rf.raftState.currentTerm,
			Command: command,
		})
		rf.logLogPrintf("recved command %v\n", command)
		term = rf.raftState.currentTerm
	}
	rf.raftState.rUnlock()
	rf.logRw.Unlock()

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
		rf.raftState.rLock()
		curState := rf.raftState.state
		rf.raftState.rUnlock()
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

	//rf.currentTerm = 0
	//rf.votedFor = -1
	rf.raftState = MakeRaftState(rf)
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.logRw)

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

	// time stamps
	rf.lastAppendEntriesTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// init seed at make time
	rand.Seed(time.Now().UnixNano())

	// log flags
	rf.electLogFlag = true
	rf.logLogFlag = true

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.mainLoop()

	// start periodical executor on commit
	go rf.tryApplyLoop()
	// start periodical commit checker
	//go rf.tryCommitLoop()

	return rf
}
