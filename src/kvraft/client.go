package kvraft

import (
	"6.824/labrpc"
	"6.824/raftservice"
	"time"
)
import "crypto/rand"
import "math/big"

//var globalClerkIdGenerator raftservice.IdGenerator

type Clerk struct {
	/*servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	opIdGenerator raftservice.IdGenerator
	myId          int
	lastLeader    int
	// lock on lastLeader
	mu deadlock.Mutex*/
	raftClerk *raftservice.RaftClerk
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.raftClerk = raftservice.MakeRaftClerk(servers)
	ck.print("initialized a clerk id %d", ck.raftClerk.Id())
	return ck
}

func (ck *Clerk) print(format string, vars ...interface{}) {
	ck.raftClerk.Print(format, vars...)
}

func (ck *Clerk) sendGet(server int, args *KvCommandArgs, reply *KvCommandReply) raftservice.RpcReply {
	//ck.print("trying to send Get to server %d", server)
	return ck.raftClerk.SendRpc(server, "KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(server int, args *KvCommandArgs, reply *KvCommandReply) raftservice.RpcReply {
	//ck.print("trying to send PutAppend to server %d", server)
	return ck.raftClerk.SendRpc(server, "KVServer.PutAppend", args, reply)
}

// reuturns whether server is leader and reachable in the operation
/*func (ck *Clerk) sendSingleCommand(getOrNot bool, server int, args *KvCommandArgs, reply *KvCommandReply) bool {
	var ok bool
	if getOrNot {
		//ck.print("sending Get key %s to server %d", args.Key, server)
		ok = ck.sendGet(server, args, reply)
	} else {
		//ck.print("sending PutAppend key %s val %s to server %d", args.Key, args.Value, server)
		ok = ck.sendPutAppend(server, args, reply)
	}
	if ok {
		if reply.Err == OK {
			return true
		}
		if reply.Err == ErrNoKey {
			return true
		}
		if reply.Err == ErrNotCommitted {
			return true
		}
		if reply.Err == ErrWrongLeader {
			return false
		}
		panic("undefined Err message")
	}
	ck.print("server %d unreachable", server)
	return false
}

const kvRpcWaitMs int = 20

type _AsyncReply struct {
	reply  *KvCommandReply
	server int
}

func (ck *Clerk) sendCommandToAll(getOrNot bool, args *KvCommandArgs) *KvCommandReply {
	replyCh := make(chan _AsyncReply)
	for i := 0; i < len(ck.servers); i++ {
		i := i
		go func() {
			reply := KvCommandReply{}
			isLeader := ck.sendSingleCommand(getOrNot, i, args, &reply)
			if isLeader {
				replyCh <- _AsyncReply{
					reply:  &reply,
					server: i,
				}
			} else {
				replyCh <- _AsyncReply{
					reply:  nil,
					server: i,
				}
			}
		}()
	}
	var reply *KvCommandReply = nil
	leader := -1
	for i := 0; i < len(ck.servers); i++ {
		newReply := <-replyCh
		if newReply.reply != nil {
			if reply != nil {
				ck.print("%d claims to be a leader, already found a leader %d", newReply.server, leader)
				ck.lastLeader = -1
				return nil
			} else {
				reply = newReply.reply
				leader = newReply.server
				break
			}
		}
	}
	ck.lastLeader = leader
	return reply
}

func (ck *Clerk) sendCommandToLeader(getOrNot bool, args *KvCommandArgs) *KvCommandReply {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.lastLeader != -1 {
		//ck.print("opid %d try first to send to last leader %d", args.OpId, ck.lastLeader)
		reply := KvCommandReply{}
		isLeader := ck.sendSingleCommand(getOrNot, ck.lastLeader, args, &reply)
		if isLeader {
			var cmdName string
			if getOrNot {
				cmdName = "Get"
			} else {
				cmdName = "PutAppend"
			}
			ck.print("opid %d %s replied %s at index %d term %d, leader %d unchanged", args.OpId, cmdName, reply.Err, reply.CommitIndex, reply.Term, ck.lastLeader)
			return &reply
		}
	}
	for {
		reply := ck.sendCommandToAll(getOrNot, args)
		if reply != nil {
			return reply
		}
		time.Sleep(time.Duration(kvRpcWaitMs) * time.Millisecond)
	}
}*/

func leaderServerReachable(reply *KvCommandReply) bool {
	if reply.Err == OK {
		return true
	}
	if reply.Err == ErrNoKey {
		return true
	}
	if reply.Err == ErrNotCommitted {
		return true
	}
	if reply.Err == ErrWrongLeader {
		return false
	}
	panic("undefined Err message")
}

const kvRpcRepeatWaitMs = 50

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := KvCommandArgs{
		Key: key,
		Op:  GetCommand,
	}
	ck.raftClerk.InitRpcArgs(&args)
	ck.print("start sending Get key [%s] opid %d", args.Key, args.OpId)
	var reply *KvCommandReply
	for {
		reply = ck.raftClerk.SendCommandToLeader(func(server int) raftservice.RpcReply {
			localReply := &KvCommandReply{}
			ck.sendGet(server, &args, localReply)
			return localReply
		}, func(rpcReply raftservice.RpcReply) bool {
			ck.print("opid %d on key [%s] value [%s] replied", args.OpId, args.Key, args.Value)
			return leaderServerReachable(rpcReply.(*KvCommandReply))
		}, func() {}).(*KvCommandReply)
		if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}
		if !reply.IsLeader {
			panic("leader finding should be handled by sendCommandToLeader")
		}
		if reply.Err == ErrNotCommitted {
			// try again
		}
		time.Sleep(time.Duration(kvRpcRepeatWaitMs) * time.Millisecond)
	}
	ck.print("Get key [%s] opid %d replied successfully", args.Key, args.OpId)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := KvCommandArgs{
		Key:   key,
		Value: value,
		Op:    PutAppendCommand,
	}
	ck.raftClerk.InitRpcArgs(&args)
	if op == "Put" {
		args.Overwrite = true
	} else if op == "Append" {
		args.Overwrite = false
	} else {
		ck.print("unknown op string %s, should only have Put or Append", op)
		panic(1)
	}
	ck.print("start sending PutAppend key [%s] value [%s] overwrite %t opid %d", args.Key, args.Value, args.Overwrite, args.OpId)
	var reply *KvCommandReply
	for {
		reply = ck.raftClerk.SendCommandToLeader(func(server int) raftservice.RpcReply {
			localReply := &KvCommandReply{}
			ck.sendPutAppend(server, &args, localReply)
			return localReply
		}, func(rpcReply raftservice.RpcReply) bool {
			ck.print("opid %d on key [%s] value [%s] replied", args.OpId, args.Key, args.Value)
			return leaderServerReachable(rpcReply.(*KvCommandReply))
		}, func() {}).(*KvCommandReply)
		if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}
		if !reply.IsLeader {
			panic("leader finding should be handled by sendCommandToLeader")
		}
		if reply.Err == ErrNotCommitted {
			// try again
		}
		time.Sleep(time.Duration(kvRpcRepeatWaitMs) * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
