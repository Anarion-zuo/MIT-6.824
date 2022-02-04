package raft

import "log"

type AddNewEntry struct {
	command interface{}
	log     *LogStateMachine
}

func (trans *AddNewEntry) transfer(source SMState) SMState {
	if source != logNormalState {
		log.Fatalln("log not at normal state")
	}
	trans.log.raft.machine.rwmu.RLock()
	trans.log.raft.print("add new log entry %v", trans.command)
	trans.log.appendLog(LogEntry{
		Command: trans.command,
		Term:    trans.log.raft.machine.currentTerm,
	})
	trans.log.raft.machine.rwmu.RUnlock()
	return notTransferred
}

func (trans *AddNewEntry) getName() string {
	return "AddNewEntry"
}

func (trans *AddNewEntry) isRW() bool {
	return true
}

func (rf *Raft) makeAddNewEntry(command interface{}) *AddNewEntry {
	return &AddNewEntry{
		command: command,
		log:     rf.log,
	}
}
