package raft

import "log"

type ApplyNew struct {
	log *LogStateMachine
}

func (sm *LogStateMachine) tryApplyRoutine(entries *[]LogEntry, begin int) {
	for i, entry := range *entries {
		//sm.raft.machine.rwmu.RLock()
		//sm.raft.print("applying command %v", entry.Command)
		//sm.raft.machine.rwmu.RUnlock()
		*sm.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: begin + i,
			// TODO snap
		}
	}
}

func (sm *LogStateMachine) tryApply() {
	applyLen := sm.commitIndex - sm.lastApplied
	if applyLen > 0 {
		//sm.raft.print("applying %d entries", applyLen)
		toBeSent := sm.log[sm.lastApplied+1 : sm.commitIndex+1]
		begin := sm.lastApplied + 1
		sm.lastApplied = sm.commitIndex
		go sm.tryApplyRoutine(&toBeSent, begin)
	}
}

func (trans *ApplyNew) transfer(source SMState) SMState {
	if logNormalState != source {
		log.Fatalln("log not at normal state")
	}

	return notTransferred
}

func (trans *ApplyNew) getName() string {
	return "ApplyNew"
}

func (trans *ApplyNew) isRW() bool {
	return true
}

func (rf *Raft) makeApplyNew() *ApplyNew {
	return &ApplyNew{
		log: rf.log,
	}
}
