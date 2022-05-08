package shardctrler

import (
	"6.824/raft"
	"6.824/raftservice"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"reflect"
)
import "6.824/labrpc"
import "6.824/labgob"

type ShardCtrler struct {
	//mu      sync.Mutex
	//me      int
	rf *raft.Raft
	//applyCh chan raft.ApplyMsg

	raftServer *raftservice.RaftServer

	// Your data here.

	rwmu deadlock.RWMutex
	//groups  [][]string
	configs []Config // indexed by config num

	printFlag bool
}

func (sc *ShardCtrler) print(format string, vars ...interface{}) {
	if sc.printFlag {
		s := fmt.Sprintf(format, vars...)
		sc.raftServer.Print("shardctrler | %s\n", s)
	}
}

func replyWrongLeader(reply *RpcReply) {
	reply.WrongLeader = true
	reply.Err = "Wrong Leader"
}

func replyTimeout(reply *RpcReply) {
	reply.WrongLeader = false
	reply.Err = "Start Timeout"
}

func replyOk(reply *RpcReply) {
	reply.WrongLeader = false
	reply.Err = "OK"
}

func replyRepeated(reply *RpcReply) {
	reply.WrongLeader = false
	reply.Err = "Repeated Operation"
}

func setReplyAttrs(reply *RpcReply, index int, term int, isLeader bool) {
	reply.SetTerm(term)
	reply.SetCommitIndex(index)
	reply.SetIsLeader(isLeader)
}

func setReplyConfigResult(reply *RpcReply, result *raftservice.OpResult) {
	config := result.Result.(*Config)
	if config != nil {
		reply.Config = *config
	}
}

func (sc *ShardCtrler) RpcHandler(args *RpcArgs, reply *RpcReply) {
	sc.raftServer.IssueCall(&args.Op, func() {
		replyWrongLeader(reply)
		setReplyAttrs(reply, -1, -1, false)
	}, func(op raftservice.RaftOp, index int, term int) {
		replyTimeout(reply)
		setReplyAttrs(reply, index, term, true)
	}, func(result *raftservice.OpResult, _op raftservice.RaftOp, index int, term int) {
		replyOk(reply)
		setReplyAttrs(reply, index, term, true)
		setReplyConfigResult(reply, result)
	}, func(op raftservice.RaftOp, term int, commitIndex int) {
		replyRepeated(reply)
		setReplyAttrs(reply, commitIndex, term, true)
	})
}

/*func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.rpcHandler(args, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.rpcHandler(args, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.rpcHandler(args, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.rpcHandler(args, reply)
}*/

func (sc *ShardCtrler) getLastConfig() *Config {
	return &sc.configs[len(sc.configs)-1]
}

func (config *Config) getGIDsAndLength() ([]int, []int) {
	result := make([]int, NShards)
	for i := 0; i < NShards; i++ {
		result[i] = config.Shards[i]
	}
	fast := 1
	slow := 0
	for ; fast < NShards; fast++ {
		if result[fast] == result[slow] {
			fast++
		} else {
			slow++
			result[slow] = result[fast]
			fast++
		}
	}
	result = result[0 : slow+1]
	lens := make([]int, slow+1)
	for i := 0; i <= slow; i++ {
		lens[i] = len(config.Groups[result[i]])
	}
	return result, lens
}

func (sc *ShardCtrler) addGroups(gids map[int][]string) {
	sc.rwmu.Lock()
	defer sc.rwmu.Unlock()

	lastConfig := sc.getLastConfig()
	oldGIDs, oldLens := lastConfig.getGIDsAndLength()
	newGIDs := make([]int, len(gids))
	newGIDLens := make([]int, len(gids))
	curIdx := 0
	for k, v := range gids {
		newGIDs[curIdx] = k
		newGIDLens[curIdx] = len(v)
		curIdx++
	}
	allGIDs := append(oldGIDs, newGIDs...)
	allGIDLens := append(oldLens, newGIDLens...)
	_, _, assignments := computeAssignment(NShards, allGIDLens)
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	for i, v := range assignments {
		newConfig.Shards[i] = allGIDs[v]
	}
	for _, v := range oldGIDs {
		newConfig.Groups[v] = lastConfig.Groups[v]
	}
	for k, v := range gids {
		newConfig.Groups[k] = v
	}
	sc.configs = append(sc.configs, newConfig)
	sc.print("new shards %v groups %v", newConfig.Shards, newConfig.Groups)
}

func (sc *ShardCtrler) removeGroups(gids []int) {
	sc.rwmu.Lock()
	defer sc.rwmu.Unlock()

	removingSet := make(map[int]bool)
	for _, v := range gids {
		removingSet[v] = true
	}
	lastConfig := sc.getLastConfig()
	oldGIDs, oldLens := lastConfig.getGIDsAndLength()
	newGIDs := make([]int, len(oldGIDs)-len(removingSet))
	newGIDLens := make([]int, len(oldGIDs)-len(removingSet))
	curIdx := 0
	for i, v := range oldGIDs {
		if removingSet[v] {
			continue
		}
		newGIDs[curIdx] = v
		newGIDLens[curIdx] = oldLens[i]
		curIdx++
	}
	_, _, assignments := computeAssignment(NShards, newGIDLens)
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	for i, v := range assignments {
		newConfig.Shards[i] = newGIDs[v]
	}
	for k, v := range lastConfig.Groups {
		if removingSet[k] {
			continue
		}
		newConfig.Groups[k] = v
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) moveShard(shard int, gid int) {
	sc.rwmu.Lock()
	defer sc.rwmu.Unlock()
	newConfig := Config{
		Num:    len(sc.configs),
		Groups: make(map[int][]string),
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = sc.getLastConfig().Shards[i]
	}
	gidSet := make(map[int]bool)
	for i := 0; i < NShards; i++ {
		gidSet[newConfig.Shards[i]] = true
	}
	newConfig.Shards[shard] = gid
	for gid, has := range gidSet {
		if !has {
			continue
		}
		newConfig.Groups[gid] = sc.getLastConfig().Groups[gid]
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) queryConfig(index int) *Config {
	sc.rwmu.RLock()
	defer sc.rwmu.RUnlock()
	var config *Config = nil
	if index < 0 {
		config = sc.getLastConfig()
	} else if index >= len(sc.configs) {
		config = sc.getLastConfig()
	} else {
		config = &sc.configs[index]
	}
	result := &Config{
		Num:    config.Num,
		Groups: make(map[int][]string),
	}
	for i := 0; i < NShards; i++ {
		result.Shards[i] = config.Shards[i]
	}
	for k, v := range config.Groups {
		if k == 0 {
			continue
		}
		newArr := make([]string, len(v))
		copy(newArr, v)
		result.Groups[k] = newArr
	}
	return result
}

func (sc *ShardCtrler) executeOp(op *Op) *Config {
	switch op.Type {
	case OpJoin:
		sc.print("joining gid mapping [%v]", op.JoinGIDMapping)
		sc.addGroups(op.JoinGIDMapping)
		return nil
	case OpLeave:
		sc.print("removing gids [%v]", op.LeaveGIDs)
		sc.removeGroups(op.LeaveGIDs)
		return nil
	case OpMove:
		sc.print("move gid %v to shard %d", op.MoveGID, op.MoveShard)
		sc.moveShard(op.MoveShard, op.MoveGID)
		return nil
	case OpQuery:
		config := sc.queryConfig(op.QueryConfigIndex)
		sc.print("query config index %d shards %v groups %v", op.QueryConfigIndex, config.Shards, config.Groups)
		return config
	default:
		panic("undefined op type")
	}
	return nil
}

func (sc *ShardCtrler) executeApplied(cmd raftservice.RaftOp, index int, term int, isLeader bool) *raftservice.OpResult {
	var op *Op = nil
	if reflect.TypeOf(cmd) == reflect.TypeOf(Op{}) {
		obj := cmd.(Op)
		op = &obj
	} else {
		op = cmd.(*Op)
	}
	//op = cmd.(*Op)
	sc.print("recved opid %d", op.OpId)
	result := sc.executeOp(op)
	//if result != nil {
	//	return &raftservice.OpResult{
	//		Result: result,
	//		Valid:  true,
	//		Term:   term,
	//	}
	//}
	return &raftservice.OpResult{
		Result: result,
		Valid:  true,
		Term:   term,
	}
}

// call Raft.Snapshot
func (sc *ShardCtrler) issueSnapshot(term int, index int) {
	//kv.rf.CondInstallSnapshot(term, index, kv.takeSnapshotWhenStateTooLarge(index))
}

// snapshot callback
func (sc *ShardCtrler) executeSnapshot(index int, snapshot []byte) {
	// switch to this snapshot
	//kv.readSnapshot(snapshot)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	//sc.rf.Kill()
	// Your code here, if desired.
	sc.raftServer.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	//sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	//sc.applyCh = make(chan raft.ApplyMsg)
	sc.raftServer = raftservice.MakeRaftServer(servers, me, persister, 1000)
	sc.rf = sc.raftServer.Raft()

	// the first config should be gid 0, invalid
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}
	sc.configs[0].Num = 0

	sc.printFlag = true

	sc.print("shardctrler server initialized")
	go sc.raftServer.PollApplyChRoutine(sc.executeApplied, sc.issueSnapshot, sc.executeSnapshot)

	return sc
}
