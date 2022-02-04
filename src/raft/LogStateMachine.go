package raft

type LogStateMachine struct {
	StateMachine

	log         []LogEntry
	commitIndex int
	lastApplied int
	applyCh     *chan ApplyMsg

	// volatile
	nextIndex  []int
	matchIndex []int

	raft *Raft
}

const logNormalState = 800

func (sm *LogStateMachine) initVolatile(peerCount int) {
	for i := 0; i < peerCount; i++ {
		sm.nextIndex[i] = sm.lastLogIndex() + 1
		sm.matchIndex[i] = 0
	}
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (sm *LogStateMachine) appendLog(entries ...LogEntry) {
	sm.log = append(sm.log, entries...)
}

func (sm *LogStateMachine) removeAfter(removeBegin int) {
	sm.log = sm.log[:removeBegin]
}

func (sm *LogStateMachine) lastLogIndex() int {
	return len(sm.log) - 1
}

func (sm *LogStateMachine) getEntry(index int) LogEntry {
	return sm.log[index]
}

/**
 * @return whether this log state is at least as up-to-date as mine
 */
func (sm *LogStateMachine) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	// If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date.
	if sm.getEntry(sm.lastLogIndex()).Term != lastLogTerm {
		return sm.getEntry(sm.lastLogIndex()).Term <= lastLogTerm
	}
	// If the logs end with the same term, then
	// whichever log is longer is more up-to-date.
	return sm.lastLogIndex() <= lastLogIndex
}

/**
 * @return the first entry index not equal to the term of the entry at prevIndex
 */
func (sm *LogStateMachine) conflictPrevIndex(prevIndex int) int {
	prevTerm := sm.getEntry(prevIndex).Term
	for i := prevIndex; i >= 0; i-- {
		if prevTerm != sm.getEntry(i).Term {
			return i
		}
	}
	return 0
}

/**
 * @return the last index having given term
 */
func (sm *LogStateMachine) backTrackLogTerm(term int) int {
	for i := sm.lastLogIndex(); i >= 1; i-- {
		if sm.getEntry(i).Term == term {
			return i
		}
	}
	return 0
}

func (rf *Raft) initLogMachine(applyCh *chan ApplyMsg) {
	rf.log = &LogStateMachine{
		StateMachine: StateMachine{
			curState: logNormalState,
			transCh:  make(chan SMTransfer),
		},
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, rf.peerCount()),
		matchIndex:  make([]int, rf.peerCount()),
		raft:        rf,
		applyCh:     applyCh,
	}
}
