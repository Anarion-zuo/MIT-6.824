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

	// raft state report
	IsLeader bool
	Term     int
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

	stateMachine *RaftStateMachine
	//logMachine    *LogStateMachine
	electionTimer *Timer
	sendAETimer   *Timer

	raftPersister *RaftPersister

	printFlag bool
}

func (rf *Raft) PeerCount() int {
	return len(rf.peers)
}

func (rf *Raft) print(format string, vars ...interface{}) {
	if !rf.printFlag {
		return
	}
	s := fmt.Sprintf(format, vars...)
	fmt.Printf("%d %s term %d votedFor %d lastLogIndex %d lastApplied %d commitIndex %d snap %d | %s\n", rf.me, rf.stateMachine.stateNameMap[rf.stateMachine.curState], rf.stateMachine.currentTerm, rf.stateMachine.votedFor, rf.stateMachine.lastLogIndex(), rf.stateMachine.lastApplied, rf.stateMachine.commitIndex, rf.stateMachine.lastSnapshotIndex, s)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.stateMachine.lockState()
	rf.stateMachine.rwmu.RLock()
	term = rf.stateMachine.currentTerm
	isleader = rf.stateMachine.curState == sendAEState
	rf.stateMachine.rwmu.RUnlock()
	//rf.stateMachine.unlockState()
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
	rf.persister.SaveRaftState(rf.raftPersister.persist(rf.stateMachine))
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
	rf.raftPersister.loadState(rf.stateMachine, data, 0)
	rf.raftPersister.loadLog(rf.stateMachine, data, statePersistOffset)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()
	if !rf.stateMachine.checkSnapshotUpToDate(lastIncludedIndex, lastIncludedTerm) {
		return false
	}
	rf.stateMachine.installSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()
	rf.stateMachine.installSnapshot(index, rf.stateMachine.getEntry(index).Term, snapshot)
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
	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.stateMachine.currentTerm

	var stateBool bool
	if args.Term > rf.stateMachine.currentTerm {
		rf.print("encounters larger term %d, transfer to follower", args.Term)
		rf.stateMachine.callTransfer(rf.makeLargerTerm(args.Term, args.CandidateId))
		stateBool = true
	} else if args.Term < rf.stateMachine.currentTerm {
		// Reply false if term < currentTerm
		rf.print("reject vote because candidate term %d less than mine %d", args.Term, rf.stateMachine.currentTerm)
		stateBool = false
	} else if rf.stateMachine.votedFor == -1 || rf.stateMachine.votedFor == args.CandidateId {
		stateBool = true
	} else {
		stateBool = false
	}
	// TODO log state
	logBool := rf.stateMachine.isUpToDate(args.LastLogIndex, args.LastLogTerm)
	if !stateBool {
		rf.print("reject vote to %d on raft state", args.CandidateId)
	}
	if !logBool {
		rf.print("reject vote to %d because candidate log not as up-to-date as mine", args.CandidateId)
	}
	if stateBool && logBool {
		reply.VoteGranted = true
		//rf.stateMachine.callTransfer(rf.makeLargerTerm(args.Term, args.CandidateId))
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

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int

	CycleId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// fast backtracking
	// the term of the conflicting entry and the first
	// index it stores for that term
	ConflictPrevTerm  int
	ConflictPrevIndex int

	CycleId int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.stateMachine.print("AppendEntries call from %d", args.LeaderId)
	reply.Success = false
	reply.CycleId = args.CycleId

	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()

	rf.print("AE recved from %d prevLogTerm %d prevLogIndex %d length %d", args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries))

	reply.Term = rf.stateMachine.currentTerm

	if args.Term > rf.stateMachine.currentTerm {
		rf.print("encounters larger term %d, transfer to follower", args.Term)
		rf.stateMachine.callTransfer(rf.makeLargerTerm(args.Term, args.LeaderId))
		reply.Success = true
		//return
	}
	// Reply false if term < currentTerm
	if args.Term < rf.stateMachine.currentTerm {
		rf.print("reply false because leader term %d less than mine %d", args.Term, rf.stateMachine.currentTerm)
		return
	}
	// candidate should transfer to follower on recving AppendEntries RPC
	if rf.stateMachine.curState == startElectionState {
		rf.stateMachine.curState = followerState
		rf.stateMachine.votedFor = args.LeaderId
		rf.stateMachine.currentTerm = args.Term
	}
	if rf.stateMachine.curState == followerState {
		rf.electionTimer.setElectionWait()
		rf.electionTimer.start()
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > rf.stateMachine.lastLogIndex() {
		rf.print("reply false prevLogIndex %d larger than lastLogIndex %d", args.PrevLogIndex, rf.stateMachine.lastLogIndex())
		reply.ConflictPrevIndex = rf.stateMachine.lastLogIndex()
		reply.ConflictPrevTerm = rf.stateMachine.getEntry(rf.stateMachine.lastLogIndex()).Term
		return
	}
	if args.PrevLogTerm != rf.stateMachine.getEntry(args.PrevLogIndex).Term {
		rf.print("reply false my log term %d not matched with leader log term %d", rf.stateMachine.getEntry(args.PrevLogIndex).Term, args.PrevLogTerm)
		reply.ConflictPrevIndex = rf.stateMachine.conflictPrevIndex(args.PrevLogIndex)
		reply.ConflictPrevTerm = rf.stateMachine.getEntry(reply.ConflictPrevIndex).Term
		return
	}
	rf.stateMachine.callTransfer(rf.makeNewAE(args.PrevLogIndex, args.Entries, args.LeaderCommit))

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
	rf.stateMachine.rwmu.Lock()
	isLeader = rf.stateMachine.curState == sendAEState
	term = rf.stateMachine.currentTerm
	index = rf.stateMachine.lastLogIndex() + 1
	if isLeader {
		// This is not correct
		// when many Start cmds come in a short period of time
		// lastLogIndex() remains the same
		// sending many cmds to be placed at the same index
		//rf.stateMachine.issueTransfer(rf.makeAddNewEntry(command))

		// The correct way of doing this is as following
		// Instead of forcing all writing behavior into state stateMachine processes
		// the state is transferred here manually
		// partly because log has only one state for now
		rf.print("recved command to index %d", index)
		rf.stateMachine.appendLog(LogEntry{
			Command: command,
			Term:    rf.stateMachine.currentTerm,
		})
		rf.sendAETimer.start()
		go rf.sendAEs()
	}
	rf.stateMachine.rwmu.Unlock()
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.stateMachine.rwmu.Lock()
	defer rf.stateMachine.rwmu.Unlock()
	rf.print("IS recved from leader %d snapshot at index %d cmd %v", args.LeaderId, args.LastIncludedIndex, args.Data)
	reply.Term = rf.stateMachine.currentTerm
	// Reply immediately if term < currentTerm
	if args.Term < rf.stateMachine.currentTerm {
		return
	}
	if args.Term > rf.stateMachine.currentTerm {
		rf.stateMachine.issueTransfer(rf.makeLargerTerm(args.Term, args.LeaderId))
	}
	// try to install
	if rf.stateMachine.lastSnapshotIndex < args.LastIncludedIndex {
		rf.stateMachine.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		if rf.stateMachine.commitIndex < args.LastIncludedIndex {
			rf.stateMachine.commitIndex = args.LastIncludedIndex
		}
		if rf.stateMachine.lastApplied < args.LastIncludedIndex {
			rf.stateMachine.lastApplied = args.LastIncludedIndex
		}
		rf.stateMachine.notifyServiceIS(args.LastIncludedIndex)
		rf.persist()
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
const electWaitMs int = 800
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

	rf.initStateMachine(applyCh)
	rf.raftPersister = makeRaftPersister()
	rf.electionTimer = makeTimer(electWaitMs, rf.makeElectionTimeout(), rf)
	rf.sendAETimer = makeTimer(sendAEWaitMs, rf.makeMajorElected(), rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// print flag
	rf.printFlag = true

	// start raft state stateMachine
	go func() {
		randMs := rand.Int() % 500
		time.Sleep(time.Duration(randMs) * time.Millisecond)

		rf.stateMachine.rwmu.RLock()
		rf.print("start election timer")
		rf.stateMachine.rwmu.RUnlock()

		rf.electionTimer.setElectionWait()
		rf.electionTimer.start()
		go rf.stateMachine.machineLoop()
	}()
	// start raft log stateMachine
	//gob.Register(raft.SMState{})

	// start ticker goroutine to start elections
	//go rf.ticker()

	return rf
}
