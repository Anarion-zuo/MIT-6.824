package raftservice

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"sync"
	"sync/atomic"
	"time"
)

type OpIdRecord struct {
	LatestOpId  int
	Term        int
	CommitIndex int
}

type RaftServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32 // set by Kill()
	applyCh      chan raft.ApplyMsg
	opWaitSet    *OpWaitSet
	maxraftstate int // snapshot if log grows this big

	// lock when calling Raft.Start
	// to prevent server from not finding conds for ops
	prepareCondMu deadlock.Mutex
	opIdMapMu     deadlock.Mutex
	latestOpIdMap map[int]*OpIdRecord // maps each opid to its

	printFlag bool
}

func (rs *RaftServer) Raft() *raft.Raft {
	return rs.rf
}

func (rs *RaftServer) Print(format string, vars ...interface{}) {
	if rs.printFlag {
		s := fmt.Sprintf(format, vars...)
		fmt.Printf("rfserver %d | %s", rs.me, s)
	}
}

func (rs *RaftServer) TakeSnapshot() (*bytes.Buffer, *labgob.LabEncoder) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rs.opIdMapMu.Lock()
	err := e.Encode(rs.latestOpIdMap)
	rs.opIdMapMu.Unlock()
	if err != nil {
		panic(err)
	}
	return w, e
}

func (rs *RaftServer) ReadSnapshot(buffer []byte) (*bytes.Buffer, *labgob.LabDecoder) {
	r := bytes.NewBuffer(buffer)
	d := labgob.NewDecoder(r)
	if len(buffer) <= 0 {
		rs.latestOpIdMap = make(map[int]*OpIdRecord)
		return r, d
	}
	rs.opIdMapMu.Lock()
	err := d.Decode(&rs.latestOpIdMap)
	rs.opIdMapMu.Unlock()
	if err != nil {
		panic(err)
	}
	return r, d
}

func (rs *RaftServer) callStart(op RaftOp) (int, int, bool) {
	rs.prepareCondMu.Lock()
	defer rs.prepareCondMu.Unlock()
	//rs.Print("start op type %v", reflect.TypeOf(op))
	index, term, isLeader := rs.rf.Start(op)
	if isLeader {
		rs.opWaitSet.AddOpWait(index)
	}
	return index, term, isLeader
}

const startTimeoutMs int = 300

func (rs *RaftServer) setOpTimeout(recvId int, timeoutResult interface{}) {
	go func() {
		time.Sleep(time.Duration(startTimeoutMs) * time.Millisecond)
		//kv.print("timeout triggered for recvid %d", recvId)
		rs.opWaitSet.DoneOp(recvId, timeoutResult, false, 0)
	}()
}

func (rs *RaftServer) WaitComplete(index int, timeoutResult interface{}) interface{} {
	rs.setOpTimeout(index, timeoutResult)
	return rs.opWaitSet.WaitOp(index)
}

func (rs *RaftServer) OpComplete(recvId int, result interface{}, term int) {
	rs.opWaitSet.DoneOp(recvId, result, true, term)
}

type applyFn func(RaftOp, int, int, bool) *OpResult

type snapshotApplyFn func(int, []byte)

type snapshotIssueFn func(int, int)

func (rs *RaftServer) ExecuteApplied(cmd RaftOp, index int, term int, isLeader bool, executor applyFn) {
	result := executor(cmd, index, term, isLeader)
	result.Term = term
	rs.prepareCondMu.Lock()
	rs.opWaitSet.DoneOp(index, result, true, 0)
	rs.prepareCondMu.Unlock()
	//kv.print("opid %d clerk %d recvid %d at index %d term %d notified rpc handler", op.OpId, op.ClerkId, op.RecvId, index, term)
}

func (rs *RaftServer) PollApplyChRoutine(executeApplied applyFn, snapshotIssue snapshotIssueFn, executeSnapshot snapshotApplyFn) {
	for {
		msg := <-rs.applyCh
		//kv.print("ApplyMsg CommandValid %t SnapshotValid %t", msg.CommandValid, msg.SnapshotValid)
		if msg.CommandValid {
			rs.ExecuteApplied(msg.Command.(RaftOp), msg.CommandIndex, msg.Term, msg.IsLeader, executeApplied)
			if rs.maxraftstate > 0 && msg.StateSize > rs.maxraftstate {
				snapshotIssue(msg.Term, msg.CommandIndex)
			}
		}
		if msg.SnapshotValid {
			rs.Print("raft applied snapshot at index %d", msg.SnapshotIndex)
			executeSnapshot(msg.SnapshotIndex, msg.Snapshot)
		}
	}
}

func (rs *RaftServer) IssueCall(op RaftOp, notLeaderFn func(),
	timeoutFn func(op RaftOp, index int, term int),
	notTimeoutFn func(result *OpResult, op RaftOp, index int, term int),
	repeatedFn func(op RaftOp, term int, commitIndex int)) {
	raftOp := op
	rs.opIdMapMu.Lock()
	record := rs.latestOpIdMap[raftOp.GetMyId()]
	rs.opIdMapMu.Unlock()
	if record != nil && raftOp.CannotRepeat() && record.LatestOpId >= raftOp.GetOpId() {
		repeatedFn(op, record.Term, record.CommitIndex)
		return
	}
	index, term, isLeader := rs.callStart(op)
	if !isLeader {
		notLeaderFn()
		return
	}
	//rs.Print("opid %d Get clerk %d index %d key %s sent to raft", op.OpId, op.ClerkId, index, op.Key)
	// wait for completion
	result := rs.WaitComplete(index, nil)
	if result == nil {
		timeoutFn(op, index, term)
	} else {
		if raftOp.CannotRepeat() {
			rs.opIdMapMu.Lock()
			rs.latestOpIdMap[raftOp.GetMyId()] = &OpIdRecord{
				LatestOpId:  raftOp.GetOpId(),
				Term:        term,
				CommitIndex: index,
			}
			rs.opIdMapMu.Unlock()
		}
		notTimeoutFn(result.(*OpResult), op, index, term)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rs *RaftServer) Kill() {
	atomic.StoreInt32(&rs.dead, 1)
	rs.rf.Kill()
	// Your code here, if desired.
	rs.Print("killed by host")
}

func (rs *RaftServer) Killed() bool {
	z := atomic.LoadInt32(&rs.dead)
	return z == 1
}

func MakeRaftServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftServer {
	rs := new(RaftServer)
	rs.me = me
	rs.maxraftstate = maxraftstate

	rs.applyCh = make(chan raft.ApplyMsg)
	rs.rf = raft.Make(servers, me, persister, rs.applyCh)
	rs.latestOpIdMap = make(map[int]*OpIdRecord)
	rs.Print("raft initialized")

	rs.opWaitSet = MakeOpWaitSet()

	rs.printFlag = true

	rs.Print("raftserver %d initialized", me)

	return rs
}
