package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   int
	Key       string
	Value     string
	Overwrite bool

	ClerkId int
	OpId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap         map[string]*ValueIndex
	mapRwmu       deadlock.RWMutex
	resultManager *ResultManager
	opWaitSet     *OpWaitSet

	persister         *KvPersister
	committedVersions map[int][]byte
	versionMutex      deadlock.Mutex

	// lock when calling Raft.Start
	// to prevent server from not finding conds for ops
	prepareCondMu deadlock.Mutex
}

type ValueIndex struct {
	Value              string
	AppliedClerkRecord map[int]int
}

func makeValueIndex(value string) *ValueIndex {
	return &ValueIndex{
		Value:              value,
		AppliedClerkRecord: make(map[int]int),
	}
}

func (old *ValueIndex) checkWriteExecutedBefore(clerkId int, opId int) bool {
	oldOpId := old.AppliedClerkRecord[clerkId]
	if opId <= oldOpId {
		return true
	}
	return false
}

func (kv *KVServer) print(format string, vars ...interface{}) {
	if Debug {
		s := fmt.Sprintf(format, vars...)
		fmt.Printf("kvserver %d | %s\n", kv.me, s)
	}
}

func (kv *KVServer) writeKV(key string, value string, overwrite bool, clerkId int, opId int) *ExecutionResult {
	result := &ExecutionResult{
		executed:  true,
		timeout:   false,
		hasKey:    true,
		vi:        ValueIndex{},
		notLeader: false,
	}
	kv.mapRwmu.Lock()
	defer kv.mapRwmu.Unlock()
	old := kv.kvMap[key]
	if old != nil && old.checkWriteExecutedBefore(clerkId, opId) {
		kv.print("opid %d by clerk %d already executed", opId, clerkId)
		return result
	}
	if overwrite {
		kv.kvMap[key] = makeValueIndex(value)
	} else {
		vi := kv.kvMap[key]
		if vi == nil {
			kv.kvMap[key] = makeValueIndex(value)
		} else {
			var sb strings.Builder
			sb.WriteString(vi.Value)
			sb.WriteString(value)
			vi.Value = sb.String()
		}
	}
	cur := kv.kvMap[key]
	if cur.AppliedClerkRecord[clerkId] < opId {
		cur.AppliedClerkRecord[clerkId] = opId
	}
	return result
}

func (kv *KVServer) executeApplied(cmd interface{}, index int, term int, isLeader bool) {
	op := cmd.(Op)
	//kv.mapRwmu.Lock()
	//defer kv.mapRwmu.Unlock()
	var result *ExecutionResult = nil
	switch op.Command {
	case GetCommand:
		// should a non-leader return Get?
		/*if !isLeader {
			kv.opWaitSet.doneOp(index, &ExecutionResult{
				executed:  false,
				timeout:   false,
				hasKey:    false,
				vi:        ValueIndex{},
				notLeader: true,
				term:      term,
			})
			kv.print("opid %d clerk %d at index %d notified rpc handler not leader", op.OpId, op.ClerkId, index)
			return
		}*/
		kv.print("execute Get key %s opid %d clerk %d at index %d", op.Key, op.OpId, op.ClerkId, index)
		result = kv.readKV(op.Key, op.ClerkId, op.OpId)
		break
	case PutAppendCommand:
		kv.print("execute PutAppend key %s value %s overwrite %t from clerk %d opid %d at index %d", op.Key, op.Value, op.Overwrite, op.ClerkId, op.OpId, index)
		result = kv.writeKV(op.Key, op.Value, op.Overwrite, op.ClerkId, op.OpId)
		break
	default:
		panic("try to execute unkown operation")
	}
	result.term = term
	kv.prepareCondMu.Lock()
	kv.opWaitSet.doneOp(index, result)
	kv.prepareCondMu.Unlock()
	//kv.print("opid %d clerk %d recvid %d at index %d term %d notified rpc handler", op.OpId, op.ClerkId, op.RecvId, index, term)
}

func (kv *KVServer) applySnapshot(index int) {

}

func (kv *KVServer) pollApplyChRoutine() {
	for {
		msg := <-kv.applyCh
		//kv.print("ApplyMsg CommandValid %t SnapshotValid %t", msg.CommandValid, msg.SnapshotValid)
		if msg.CommandValid {
			kv.executeApplied(msg.Command, msg.CommandIndex, msg.Term, msg.IsLeader)
		} else if msg.SnapshotValid {
			kv.print("raft applied snapshot at index %d", msg.SnapshotIndex)
			kv.applySnapshot(msg.SnapshotIndex)
		} else {
			// both not valid

		}
	}
}

const startTimeoutMs int = 300

func (kv *KVServer) makeOp(args *KvCommandArgs) *Op {
	result := &Op{
		Command:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Overwrite: args.Overwrite,
		ClerkId:   args.MyId,
		OpId:      args.OpId,
	}
	//kv.print("make opid %d clerk %d recvid %d key %s value %s overwrite %t", result.OpId, result.ClerkId, result.RecvId, result.Key, result.Value, result.Overwrite)
	return result
}

func (kv *KVServer) setOpTimeout(recvId int) {
	go func() {
		time.Sleep(time.Duration(startTimeoutMs) * time.Millisecond)
		kv.print("timeout triggered for recvid %d", recvId)
		kv.opWaitSet.doneOp(recvId, &ExecutionResult{
			executed: false,
			timeout:  true,
			hasKey:   false,
			vi:       ValueIndex{},
			term:     -1,
		})
	}()
}

func (kv *KVServer) callRaftStart(args *KvCommandArgs) (*Op, int, int, bool) {
	op := kv.makeOp(args)
	kv.prepareCondMu.Lock()
	defer kv.prepareCondMu.Unlock()
	index, term, isLeader := kv.rf.Start(*op)
	if isLeader {
		kv.opWaitSet.addOpWait(index)
	}
	return op, index, term, isLeader
}

func (kv *KVServer) setGetReplyErr(result *ExecutionResult, reply *KvCommandReply) {
	if result.notLeader {
		reply.Err = ErrWrongLeader
	} else if result.timeout {
		reply.Err = ErrNotCommitted
	} else if !result.hasKey {
		reply.Err = ErrNoKey
	} else if result.executed {
		reply.Err = OK
		reply.Value = result.vi.Value
	} else {
		panic("operation reported in undefined state")
	}
}

func (kv *KVServer) Get(args *KvCommandArgs, reply *KvCommandReply) {
	// Your code here.
	op, index, _, isLeader := kv.callRaftStart(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.CommitIndex = index
	kv.print("opid %d clerk %d index %d sent to raft", op.OpId, op.ClerkId, index)
	// wait for completion
	kv.setOpTimeout(index)
	result := kv.opWaitSet.waitOp(index)
	reply.Term = result.term
	kv.print("opid %d clerk %d at index %d Get wait done timeout %t", op.OpId, op.ClerkId, index, result.timeout)
	kv.setGetReplyErr(result, reply)
}

func (kv *KVServer) setPutAppendReplyErr(result *ExecutionResult, reply *KvCommandReply) {
	if result.notLeader {
		reply.Err = ErrWrongLeader
	} else if result.timeout {
		reply.Err = ErrNotCommitted
	} else if result.executed {
		reply.Err = OK
	} else {
		panic("operation reported in undefined state")
	}
}

func (kv *KVServer) PutAppend(args *KvCommandArgs, reply *KvCommandReply) {
	// Your code here.
	op, index, _, isLeader := kv.callRaftStart(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.CommitIndex = index
	kv.print("opid %d clerk %d index %d sent to raft", op.OpId, op.ClerkId, index)
	// wait for completion
	kv.setOpTimeout(index)
	result := kv.opWaitSet.waitOp(index)
	reply.Term = result.term
	kv.print("opid %d clerk %d index %d timeout %t", op.OpId, op.ClerkId, index, result.timeout)
	kv.setPutAppendReplyErr(result, reply)
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
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.print("killed by host")
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]*ValueIndex)
	kv.resultManager = makeCondManager()
	kv.committedVersions = make(map[int][]byte)
	kv.opWaitSet = makeOpWaitSet()
	//kv.initKvPersister()

	kv.print("kvserver %d initialized", me)
	go kv.pollApplyChRoutine()

	return kv
}
