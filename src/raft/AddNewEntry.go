package raft

type AddNewEntry struct {
	command interface{}
	machine *RaftStateMachine
}

func (trans *AddNewEntry) transfer(source SMState) SMState {
	trans.machine.raft.print("add new log entry %v", trans.command)
	trans.machine.appendLog(LogEntry{
		Command: trans.command,
		Term:    trans.machine.raft.stateMachine.currentTerm,
	})
	trans.machine.raft.persist()
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
		machine: rf.stateMachine,
	}
}
