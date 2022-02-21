package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"time"
)
import "crypto/rand"
import "math/big"

var globalClerkIdGenerator IdGenerator

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	opIdGenerator IdGenerator
	myId          int
	lastLeader    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.opIdGenerator.curVal = 1
	ck.myId = globalClerkIdGenerator.make()
	ck.lastLeader = -1
	ck.print("initialized a clerk id %d", ck.myId)
	return ck
}

func (ck *Clerk) print(format string, vars ...interface{}) {
	if Debug {
		s := fmt.Sprintf(format, vars...)
		fmt.Printf("clerk %d lastLeader %d | %s\n", ck.myId, ck.lastLeader, s)
	}
}

func (ck *Clerk) sendGet(server int, args *KvCommandArgs, reply *KvCommandReply) bool {
	//ck.print("trying to send Get to server %d", server)
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(server int, args *KvCommandArgs, reply *KvCommandReply) bool {
	//ck.print("trying to send PutAppend to server %d", server)
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}

// reuturns whether server is leader and reachable in the operation
func (ck *Clerk) sendSingleCommand(getOrNot bool, server int, args *KvCommandArgs, reply *KvCommandReply) bool {
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

/*
func (ck *Clerk) sendCommandToAll(getOrNot bool, args *KvCommandArgs) (*KvCommandReply, int) {
	replies := make([]KvCommandReply, len(ck.servers))
	cond := sync.NewCond(&sync.Mutex{})
	joinCount := 0
	leader := -1
	for i, _ := range ck.servers {
		go ck.sendSingleCommand(getOrNot, i, args, &replies[i], cond, &joinCount, &leader)
	}
	cond.L.Lock()
	defer cond.L.Unlock()
	for !(joinCount >= len(ck.servers) || leader != -1) {
		cond.Wait()
	}
	if leader == -1 {
		return nil, -1
	}
	return &replies[leader], leader
}*/

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
		//ck.print("opid %d try to send to all", args.OpId)
		/*for i := 0; i < len(ck.servers); i++ {
			reply := KvCommandReply{}
			isLeader := ck.sendSingleCommand(getOrNot, i, args, &reply)
			if isLeader {
				ck.lastLeader = i
				var cmdName string
				if getOrNot {
					cmdName = "Get"
				} else {
					cmdName = "PutAppend"
				}
				ck.print("opid %d %s replied %s at index %d term %d updated leader to %d", args.OpId, cmdName, reply.Err, reply.CommitIndex, reply.Term, ck.lastLeader)
				return &reply
			}
		}*/
		reply := ck.sendCommandToAll(getOrNot, args)
		if reply != nil {
			return reply
		}
		time.Sleep(time.Duration(kvRpcWaitMs) * time.Millisecond)
	}
}

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
		Key:  key,
		Op:   GetCommand,
		OpId: ck.opIdGenerator.make(),
		MyId: ck.myId,
	}
	ck.print("start sending Get key %s opid %d", args.Key, args.OpId)
	var reply *KvCommandReply
	for {
		reply = ck.sendCommandToLeader(true, &args)
		if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}
		if reply.Err == ErrWrongLeader {
			panic("leader finding should be handled by sendCommandToLeader")
		}
		if reply.Err == ErrNotCommitted {
			// try again
		}
	}
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
		OpId:  ck.opIdGenerator.make(),
		MyId:  ck.myId,
	}
	if op == "Put" {
		args.Overwrite = true
	} else if op == "Append" {
		args.Overwrite = false
	} else {
		ck.print("unknown op string %s, should only have Put or Append", op)
		panic(1)
	}
	ck.print("start sending PutAppend key %s value %s overwrite %t opid %d", args.Key, args.Value, args.Overwrite, args.OpId)
	var reply *KvCommandReply
	for {
		reply = ck.sendCommandToLeader(false, &args)
		if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}
		if reply.Err == ErrWrongLeader {
			panic("leader finding should be handled by sendCommandToLeader")
		}
		if reply.Err == ErrNotCommitted {
			// try again
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
