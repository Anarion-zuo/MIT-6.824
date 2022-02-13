package raft

import (
	"6.824/labgob"
	"bytes"
)

const statePersistOffset int = 100

type RaftPersister struct {
}

type RaftPersistState struct {
	CurrentTerm int
	VotedFor    int
}

func makeRaftPersister() *RaftPersister {
	return &RaftPersister{}
}

func (rp *RaftPersister) serializeState(stateMachine *RaftStateMachine) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(RaftPersistState{
		CurrentTerm: stateMachine.currentTerm,
		VotedFor:    stateMachine.votedFor,
	})
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rp *RaftPersister) loadState(stateMahine *RaftStateMachine, buffer []byte, offset int) {
	r := bytes.NewBuffer(buffer[offset:])
	d := labgob.NewDecoder(r)
	decoded := RaftPersistState{}
	err := d.Decode(&decoded)
	if err != nil {
		panic(err)
	}
	stateMahine.currentTerm = decoded.CurrentTerm
	stateMahine.votedFor = decoded.VotedFor
}

func (rp *RaftPersister) serializeLog(machine *RaftStateMachine) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(machine.log)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rp *RaftPersister) loadLog(machine *RaftStateMachine, buffer []byte, offset int) {
	r := bytes.NewBuffer(buffer[offset:])
	d := labgob.NewDecoder(r)
	decoded := make([]LogEntry, 0)
	err := d.Decode(&decoded)
	if err != nil {
		panic(err)
	}
	machine.log = decoded
}

func (rp *RaftPersister) persist(stateMachine *RaftStateMachine) []byte {
	stateBytes := rp.serializeState(stateMachine)
	if len(stateBytes) > statePersistOffset {
		panic("serialized state byte count more than manually set boundary")
	}
	logBytes := rp.serializeLog(stateMachine)
	image := make([]byte, statePersistOffset+len(logBytes))
	for i, b := range stateBytes {
		image[i] = b
	}
	for i, b := range logBytes {
		image[i+statePersistOffset] = b
	}
	return image
	//persister.SaveRaftState(image)
}
