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
	"fmt"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	machine       *RaftStateMachine
	electionTimer *Timer
	sendAETimer   *Timer
}

func (rf *Raft) peerCount() int {
	return len(rf.peers)
}

type RaftStateMachine struct {
	StateMachine

	raft        *Raft
	currentTerm int
	votedFor    int

	stateNameMap map[SMState]string

	printMu   sync.Mutex
	printFlag bool
}

func (sm *RaftStateMachine) print(format string, vars ...interface{}) {
	if !sm.printFlag {
		return
	}
	sm.printMu.Lock()
	s := fmt.Sprintf(format, vars...)
	fmt.Printf("%d %s term %d votedFor %d | %s\n", sm.raft.me, sm.stateNameMap[sm.curState], sm.currentTerm, sm.votedFor, s)
	sm.printMu.Unlock()
}

func (sm *RaftStateMachine) registerSingleState(state SMState, name string) {
	if name2, ok := sm.stateNameMap[state]; ok {
		log.Fatalf("state %d %s already in name map\n", state, name2)
	}
	sm.stateNameMap[state] = name
}

func (sm *RaftStateMachine) registerStates() {
	sm.registerSingleState(startElectionState, "StartElection")
	sm.registerSingleState(followerState, "Follower")
	sm.registerSingleState(sendAEState, "SendAE")
}

type RaftStateWriter struct {
	machine *RaftStateMachine
}

func (sw *RaftStateWriter) writeState(dest SMState) {
	sw.machine.printMu.Lock()
	sw.machine.curState = dest
	sw.machine.printMu.Unlock()
}

type RaftTransferExecutor struct {
	machine *RaftStateMachine
}

func (e *RaftTransferExecutor) executeTransfer(source SMState, trans SMTransfer) SMState {
	//e.machine.print("execute %s", trans.getName())
	nextState := trans.transfer(source)
	return nextState
}

func (rf *Raft) initStateMachine() {
	rf.machine = &RaftStateMachine{
		StateMachine: StateMachine{
			curState: followerState, // initial state
			transCh:  make(chan SMTransfer),
		},
		raft:         rf,
		currentTerm:  0,
		votedFor:     -1,
		stateNameMap: make(map[SMState]string),
	}
	rf.machine.transferExecutor = &RaftTransferExecutor{machine: rf.machine}
	rf.machine.stateWriter = &RaftStateWriter{machine: rf.machine}
	rf.machine.registerStates()
}

const startElectionState SMState = 900
const followerState SMState = 901
const sendAEState SMState = 902

type RaftStateTransfer struct {
	machine *RaftStateMachine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.machine.lockState()
	rf.machine.rwmu.RLock()
	term = rf.machine.currentTerm
	isleader = rf.machine.curState == sendAEState
	rf.machine.rwmu.RUnlock()
	//rf.machine.unlockState()
	return term, isleader
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
	rf.machine.rwmu.RLock()
	defer rf.machine.rwmu.RUnlock()

	reply.VoteGranted = false
	reply.Term = rf.machine.currentTerm
	if args.Term > rf.machine.currentTerm {
		rf.machine.print("encounters larger term %d, transfer to follower, grant vote", args.Term)
		rf.machine.issueTransfer(rf.makeLargerTerm(args.Term, args.CandidateId))
		reply.VoteGranted = true
		return
	}
	// Reply false if term < currentTerm
	if args.Term < rf.machine.currentTerm {
		rf.machine.print("reject vote because candidate term %d less than mine %d", args.Term, rf.machine.currentTerm)
		return
	}
	stateBool := rf.machine.votedFor == -1 || rf.machine.votedFor == args.CandidateId
	// TODO log state
	if stateBool {
		reply.VoteGranted = true
		rf.machine.issueTransfer(rf.makeLargerTerm(args.Term, args.CandidateId))
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// TODO log fields
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.machine.print("AppendEntries call from %d", args.LeaderId)
	reply.Success = false
	rf.electionTimer.setElectionWait()
	rf.electionTimer.start()

	rf.machine.rwmu.RLock()
	defer rf.machine.rwmu.RUnlock()

	reply.Term = rf.machine.currentTerm

	if args.Term > rf.machine.currentTerm {
		rf.machine.print("encounters larger term %d, transfer to follower", args.Term)
		rf.machine.issueTransfer(rf.makeLargerTerm(args.Term, args.LeaderId))
		return
	}
	// Reply false if term < currentTerm
	if args.Term < rf.machine.currentTerm {
		rf.machine.print("reply false because candidate term %d less than mine %d", args.Term, rf.machine.currentTerm)
		return
	}
	// TODO log
	reply.Success = true
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	if rf.killed() == false {

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
const electWaitMs int = 400
const electWaitRandomOffsetMs int = 100
const sendAEWaitMs int = 100

func (timer *Timer) setElectionWait() {
	timer.setWaitMs(electWaitMs + rand.Int()%electWaitRandomOffsetMs)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// rand seed
	go func() {
		for {
			rand.Seed(time.Now().UnixNano())
			randMs := 800 + rand.Int()%1000
			time.Sleep(time.Duration(randMs) * time.Millisecond)
		}
	}()

	rf.initStateMachine()
	rf.electionTimer = makeTimer(electWaitMs, rf.makeElectionTimeout(), rf)
	rf.sendAETimer = makeTimer(sendAEWaitMs, rf.makeMajorElected(), rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// print flag
	rf.machine.printFlag = true

	go func() {
		randMs := rand.Int() % 500
		time.Sleep(time.Duration(randMs) * time.Millisecond)
		rf.machine.print("start election timer")
		rf.electionTimer.setElectionWait()
		rf.electionTimer.start()
		go rf.machine.machineLoop()
	}()

	// start ticker goroutine to start elections
	//go rf.ticker()

	return rf
}
