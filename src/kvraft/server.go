package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/raftservice"
	"github.com/sasha-s/go-deadlock"
	"log"
	"strings"
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
	rf         *raft.Raft
	raftServer *raftservice.RaftServer

	// Your definitions here.
	kvMap         map[string]*ValueIndex
	mapRwmu       deadlock.RWMutex
	resultManager *ResultManager

	versionMutex deadlock.Mutex

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
	kv.raftServer.Print(format, vars...)
}

func (kv *KVServer) writeKV(key string, value string, overwrite bool, clerkId int, opId int) *ExecutionResult {
	result := &ExecutionResult{
		Executed:  true,
		Timeout:   false,
		HasKey:    true,
		Vi:        ValueIndex{},
		NotLeader: false,
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

func (kv *KVServer) executeApplied(cmd interface{}, index int, term int, isLeader bool) *raftservice.OpResult {
	op := cmd.(Op)
	//kv.mapRwmu.Lock()
	//defer kv.mapRwmu.Unlock()
	var result *ExecutionResult = nil
	switch op.Command {
	case GetCommand:
		// should a non-leader return Get?
		kv.print("execute Get key [%s] opid %d clerk %d at index %d", op.Key, op.OpId, op.ClerkId, index)
		result = kv.readKV(op.Key, op.ClerkId, op.OpId)
		break
	case PutAppendCommand:
		kv.print("execute PutAppend key [%s] value [%s] overwrite %t from clerk %d opid %d at index %d", op.Key, op.Value, op.Overwrite, op.ClerkId, op.OpId, index)
		result = kv.writeKV(op.Key, op.Value, op.Overwrite, op.ClerkId, op.OpId)
		break
	default:
		panic("try to execute unkown operation")
	}
	return &raftservice.OpResult{
		Result: result,
		Valid:  true,
		Term:   term,
	}
}

func (kv *KVServer) makeOp(args *KvCommandArgs) *Op {
	result := &Op{
		Command:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Overwrite: args.Overwrite,
		ClerkId:   args.MyId,
		OpId:      args.OpId,
	}
	//kv.print("make opid %d clerk %d recvid %d key [%s] value [%s] overwrite %t", result.OpId, result.ClerkId, result.RecvId, result.Key, result.Value, result.Overwrite)
	return result
}

func (kv *KVServer) setGetReplyErr(result *ExecutionResult, reply *KvCommandReply, timeout bool) {
	if timeout {
		reply.Err = ErrNotCommitted
	} else if !result.HasKey {
		reply.Err = ErrNoKey
	} else if result.Executed {
		reply.Err = OK
		reply.Value = result.Vi.Value
	} else {
		panic("operation reported in undefined state")
	}
}

func (kv *KVServer) Get(args *KvCommandArgs, reply *KvCommandReply) {
	// Your code here.
	reply.IsLeader = true
	kv.raftServer.IssueCall(*kv.makeOp(args), func() {
		reply.IsLeader = false
		reply.Err = ErrWrongLeader
	}, func(_op interface{}, index int, term int) {
		op := _op.(Op)
		kv.print("opid %d clerk %d at index %d Get wait done timeout true", op.OpId, op.ClerkId, index)
		kv.setGetReplyErr(nil, reply, true)
	}, func(result *raftservice.OpResult, _op interface{}, index int, term int) {
		op := _op.(Op)
		reply.Term = term
		reply.CommitIndex = index
		kv.setGetReplyErr(result.Result.(*ExecutionResult), reply, false)
		kv.print("opid %d clerk %d at index %d Get wait done timeout false", op.OpId, op.ClerkId, index)
	})
}

func (kv *KVServer) setPutAppendReplyErr(result *ExecutionResult, reply *KvCommandReply, timeout bool) {
	if timeout {
		reply.Err = ErrNotCommitted
	} else if result.Executed {
		reply.Err = OK
	} else {
		panic("operation reported in undefined state")
	}
}

func (kv *KVServer) PutAppend(args *KvCommandArgs, reply *KvCommandReply) {
	// Your code here.
	reply.IsLeader = true
	kv.raftServer.IssueCall(*kv.makeOp(args), func() {
		reply.IsLeader = false
		reply.Err = ErrWrongLeader
	}, func(_op interface{}, index int, term int) {
		op := _op.(Op)
		kv.print("opid %d clerk %d at index %d PutAppend wait done timeout true", op.OpId, op.ClerkId, index)
		kv.setPutAppendReplyErr(nil, reply, true)
	}, func(result *raftservice.OpResult, _op interface{}, index int, term int) {
		op := _op.(Op)
		reply.Term = term
		reply.CommitIndex = index
		kv.setGetReplyErr(result.Result.(*ExecutionResult), reply, false)
		kv.print("opid %d clerk %d at index %d PutAppend wait done timeout false", op.OpId, op.ClerkId, index)
	})
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
	kv.raftServer.Kill()
}

func (kv *KVServer) killed() bool {
	return kv.raftServer.Killed()
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

	kv.raftServer = raftservice.MakeRaftServer(servers, me, persister, maxraftstate)
	kv.rf = kv.raftServer.Raft()

	//kv.applyCh = make(chan raft.ApplyMsg)
	kv.print("raft initialized")
	kv.kvMap = make(map[string]*ValueIndex)
	kv.resultManager = makeResultManager()

	kv.print("kvserver %d initialized", me)
	go kv.raftServer.PollApplyChRoutine(kv.executeApplied, kv.issueSnapshot, kv.executeSnapshot)

	return kv
}
