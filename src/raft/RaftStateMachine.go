package raft

import (
	"log"
	"sync"
)

type RaftStateMachine struct {
	StateMachine

	raft        *Raft
	currentTerm int
	votedFor    int

	log         []LogEntry
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond

	// volatile
	nextIndex  []int
	matchIndex []int

	// snap
	lastSnapshotIndex int

	stateNameMap map[SMState]string
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
	sw.machine.curState = dest
}

type RaftTransferExecutor struct {
	machine *RaftStateMachine
}

func (e *RaftTransferExecutor) executeTransfer(source SMState, trans SMTransfer) SMState {
	//e.stateMachine.print("execute %s", trans.getName())
	nextState := trans.transfer(source)
	return nextState
}

func (rf *Raft) initStateMachine(applyCh chan ApplyMsg) {
	rf.stateMachine = &RaftStateMachine{
		StateMachine: StateMachine{
			curState: followerState, // initial state
			transCh:  make(chan SMTransfer),
		},
		raft:         rf,
		currentTerm:  0,
		votedFor:     -1,
		stateNameMap: make(map[SMState]string),
		// log
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, rf.PeerCount()),
		matchIndex:  make([]int, rf.PeerCount()),
		applyCh:     applyCh,
	}
	rf.stateMachine.applyCond = sync.NewCond(&rf.stateMachine.rwmu)
	rf.stateMachine.registerStates()
	go rf.stateMachine.applyRoutine()
}

const startElectionState SMState = 900
const followerState SMState = 901
const sendAEState SMState = 902

type RaftStateTransfer struct {
	machine *RaftStateMachine
}
