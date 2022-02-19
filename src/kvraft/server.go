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

	Executed bool
	CondId   int
}

type WaitItem struct {
	cond     *sync.Cond
	executed *bool
	timeout  *bool
}

type CondManager struct {
	conds map[int]WaitItem
	curId int
	mu    deadlock.Mutex
}

func makeCondManager() *CondManager {
	return &CondManager{
		conds: make(map[int]WaitItem),
		curId: 0,
	}
}

func (m *CondManager) make() (int, *sync.Cond) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.curId
	m.curId++
	cond := sync.NewCond(&deadlock.Mutex{})
	executed := false
	commited := false
	m.conds[id] = WaitItem{
		cond:     cond,
		executed: &executed,
		timeout:  &commited,
	}
	return id, cond
}

func (m *CondManager) get(id int) WaitItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conds[id]
}

func (m *CondManager) release(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conds, id)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap   map[string]*ValueIndex
	mapRwmu deadlock.RWMutex
	//opManager     *ServerOpIdManager
	//opManagerCond *sync.Cond
	//idMap       map[ServerOpId]int
	//idMapMu     deadlock.Mutex
	condManager *CondManager
}

type ValueIndex struct {
	value              string
	appliedClerkRecord map[int]int
}

func makeValueIndex(value string) *ValueIndex {
	return &ValueIndex{
		value:              value,
		appliedClerkRecord: make(map[int]int),
	}
}

func (old *ValueIndex) checkExecutedBefore(clerkId int, opId int) bool {
	oldOpId := old.appliedClerkRecord[clerkId]
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

func (kv *KVServer) writeKV(key string, value string, overwrite bool, clerkId int, opId int) {
	old := kv.kvMap[key]
	if old != nil && old.checkExecutedBefore(clerkId, opId) {
		kv.print("command id %d by clerk %d on key %s value %s already executed", opId, clerkId, key, value)
		return
	}
	kv.print("execute PutAppend key %s value %s overwrite %t from clerk %d opid %d", key, value, overwrite, clerkId, opId)
	if overwrite {
		kv.kvMap[key] = makeValueIndex(value)
	} else {
		vi := kv.kvMap[key]
		if vi == nil {
			kv.kvMap[key] = makeValueIndex(value)
		} else {
			var sb strings.Builder
			sb.WriteString(vi.value)
			sb.WriteString(value)
			vi.value = sb.String()
		}
	}
	cur := kv.kvMap[key]
	if cur.appliedClerkRecord[clerkId] < opId {
		cur.appliedClerkRecord[clerkId] = opId
	}
}

func (kv *KVServer) executeApplied(cmd interface{}, index int) {
	op := cmd.(Op)
	//kv.opManagerCond.L.Lock()
	//defer kv.opManagerCond.L.Unlock()
	//kv.idMapMu.Lock()
	//defer kv.idMapMu.Unlock()
	switch op.Command {
	case GetCommand:
		kv.print("execute Get key %s", op.Key)
		break
	case PutAppendCommand:
		kv.mapRwmu.Lock()
		kv.writeKV(op.Key, op.Value, op.Overwrite, op.ClerkId, op.OpId)
		kv.mapRwmu.Unlock()
		break
	default:
		panic("try to execute unkown operation")
	}
	//kv.idMap[key] = index
	item := kv.condManager.get(op.CondId)
	cond := item.cond
	if cond == nil {
		kv.print("no need to notify because I am not leader")
		return
	}
	cond.L.Lock()
	*item.executed = true
	*item.timeout = false
	cond.Broadcast()
	cond.L.Unlock()
	kv.print("notified opid %d from clerk %d done", op.OpId, op.ClerkId)
}

func (kv *KVServer) pollApplyChRoutine() {
	for {
		msg := <-kv.applyCh
		//kv.print("ApplyMsg CommandValid %t SnapshotValid %t", msg.CommandValid, msg.SnapshotValid)
		if msg.CommandValid {
			kv.executeApplied(msg.Command, msg.CommandIndex)
		} else if msg.SnapshotValid {

		} else {
			// both not valid

		}
	}
}

const startTimeoutMs int = 300

/**
 * Unified operation entry
 * @returns whether the server is leader currently
			and the value for a potential Get call.
			and whether raft successfully committed
*/
func (kv *KVServer) startOp(op *Op) (bool, *ValueIndex, bool) {
	commitIndex, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return false, nil, false
	}
	kv.print("key %s value %s sent to raft opid %d from clerk %d", op.Key, op.Value, op.OpId, op.ClerkId)
	// code goes here if this server is leader
	// after this command is submitted
	item := kv.condManager.get(op.CondId)
	cond := item.cond
	go func() {
		time.Sleep(time.Duration(startTimeoutMs) * time.Millisecond)
		cond.L.Lock()
		*item.executed = false
		*item.timeout = true
		cond.Broadcast()
		cond.L.Unlock()
	}()
	cond.L.Lock()
	for !(*item.executed || *item.timeout) {
		cond.Wait()
	}
	executed := *item.executed
	timeout := *item.timeout
	cond.L.Unlock()
	kv.condManager.release(op.CondId)
	if executed {
		kv.print("opid %d from clerk %d executed at index %d", op.OpId, op.ClerkId, commitIndex)
		// read what has just been executed
		kv.mapRwmu.RLock()
		defer kv.mapRwmu.RUnlock()
		vip := kv.kvMap[op.Key]
		if vip == nil {
			return true, nil, true
		}
		vi := *vip
		return true, &vi, true
	}
	if timeout {
		kv.print("opid %d from clerk %d timeout after %dms, report to clerk not committed", op.OpId, op.ClerkId, startTimeoutMs)
		return true, nil, false
	}
	kv.print("not execute and not timeout, code should not reach here")
	panic(1)
}

func (kv *KVServer) makeOp(args *KvCommandArgs) *Op {
	result := &Op{
		Command:   PutAppendCommand,
		Key:       args.Key,
		Value:     args.Value,
		Overwrite: args.Overwrite,
		ClerkId:   args.MyId,
		OpId:      args.OpId,
		Executed:  false,
	}
	result.CondId, _ = kv.condManager.make()
	return result
}

func (kv *KVServer) Get(args *KvCommandArgs, reply *KvCommandReply) {
	// Your code here.
	op := kv.makeOp(args)
	isLeader, value, committed := kv.startOp(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if !committed {
		reply.Err = ErrNotCommitted
	} else if value == nil {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = value.value
	}
	//kv.print("reply %s", reply.Err)
}

func (kv *KVServer) PutAppend(args *KvCommandArgs, reply *KvCommandReply) {
	// Your code here.
	op := kv.makeOp(args)
	isLeader, _, committed := kv.startOp(op)
	if !committed {
		reply.Err = ErrNotCommitted
	} else if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
	//kv.print("reply %s", reply.Err)
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
	//kv.opManagerCond = sync.NewCond(&deadlock.Mutex{})
	kv.kvMap = make(map[string]*ValueIndex)
	//kv.idMap = make(map[ServerOpId]int)
	//kv.opManager = makeServerOpIdManager(len(servers))
	kv.condManager = makeCondManager()
	go kv.pollApplyChRoutine()

	return kv
}
