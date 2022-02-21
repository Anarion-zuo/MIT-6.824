package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
	"sync"
)

/**
 * Check whether my log at index index is the same with snapshot.
 */
func (sm *RaftStateMachine) panicIfSnapshotInvalid(index int, snapshot []byte) {
	// index can be larger than my lastLogIndex
	// can fast forward in this way
	if index > sm.lastLogIndex() {
		return
	}
	// must not be earlier than last time
	if index < sm.lastSnapshotIndex {
		log.Panicf("snapshotting index %d smaller than last snapshot index %d", index, sm.lastSnapshotIndex)
	}
	// must check if index is within my range
	my := sm.getEntry(index).Command.(int)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var s int
	err := d.Decode(&s)
	if err != nil {
		log.Panicln("encode command error")
	}
	if my != s {
		log.Panicf("snapshotting at index %d value %d not equal to my log %d", index, s, sm.getEntry(index).Command.(int))
	}
}

func (sm *RaftStateMachine) physicalIndex(index int) int {
	return index - sm.lastSnapshotIndex
}

func (sm *RaftStateMachine) fastForwardSnapshot(term int, snapshot []byte) {
	sm.log = make([]LogEntry, 1)
	newEntry := LogEntry{
		Term: term,
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var decoded int
	err := d.Decode(&decoded)
	if err != nil {
		log.Panicf("decode snapshot command failed: %v", err)
	}
	newEntry.Command = decoded
	sm.log[0] = newEntry
}

func (sm *RaftStateMachine) trimLog(index int) {
	newLog := make([]LogEntry, sm.lastLogIndex()-index+1)
	copy(newLog, sm.log[sm.physicalIndex(index):])
	sm.log = newLog
}

func (sm *RaftStateMachine) installSnapshot(index int, term int, snapshot []byte) {
	if index < sm.lastSnapshotIndex {
		// I already have a snapshot for you
		// do nothing
		sm.raft.print("snapshot request at index %d smaller than current snapshotIndex %d, ignoring...", index, sm.lastSnapshotIndex)
		return
	}
	sm.panicIfSnapshotInvalid(index, snapshot)
	if index > sm.lastLogIndex() {
		sm.raft.print("fast-forward snapshot to index %d", index)
		sm.fastForwardSnapshot(term, snapshot)
	} else {
		sm.raft.print("trim log with snapshot at index %d", index)
		sm.trimLog(index)
	}
	sm.lastSnapshotIndex = index
	sm.raft.persist()
}

func (sm *RaftStateMachine) checkSnapshotUpToDate(index int, term int) bool {
	myTerm := sm.getEntry(sm.lastApplied).Term
	if term > myTerm {
		return true
	}
	if term < myTerm {
		return false
	}
	// term == myTerm
	return index >= sm.lastApplied
}

func (sm *RaftStateMachine) lastSnapshotTerm() int {
	return sm.getEntry(sm.lastSnapshotIndex).Term
}

func (sm *RaftStateMachine) lastSnapshotCommand() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(sm.getEntry(sm.lastSnapshotIndex).Command.(int)) != nil {
		panic("failed to encode command")
	}
	return w.Bytes()
}

func (rf *Raft) sendSingleIS(server int, joinCount *int, cond *sync.Cond) {
	rf.stateMachine.rwmu.RLock()
	args := InstallSnapshotArgs{
		Term:              rf.stateMachine.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.stateMachine.lastSnapshotIndex,
		LastIncludedTerm:  rf.stateMachine.lastSnapshotTerm(),
		Data:              rf.stateMachine.lastSnapshotCommand(),
	}
	rf.stateMachine.rwmu.RUnlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok {
		rf.stateMachine.rwmu.Lock()
		defer rf.stateMachine.rwmu.Unlock()
		if reply.Term > rf.stateMachine.currentTerm {
			rf.stateMachine.issueTransfer(rf.makeLargerTerm(reply.Term, server))
			return
		}
		rf.stateMachine.tryUpdateVolatileBySnapshot(server, rf.stateMachine.lastSnapshotIndex)
	}
	cond.L.Lock()
	*joinCount++
	if *joinCount+1 >= rf.PeerCount() {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

func (sm *RaftStateMachine) tryUpdateVolatileBySnapshot(server int, index int) {
	if sm.nextIndex[server]-1 < index {
		sm.nextIndex[server] = index + 1
	}
	if sm.matchIndex[server] < sm.nextIndex[server]-1 {
		sm.matchIndex[server] = sm.nextIndex[server] - 1
	}
	sm.tryCommit()
}

func (sm *RaftStateMachine) notifyServiceIS(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(sm.getEntry(index).Command.(int))
	if err != nil {
		log.Panicf("encode command failed: %v", err)
	}
	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      w.Bytes(),
		SnapshotTerm:  sm.getEntry(index).Term,
		SnapshotIndex: index,
	}
	go func() {
		*sm.applyCh <- *msg
	}()
}
