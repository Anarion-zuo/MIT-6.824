package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

var globalClerkIdGenerator IdGenerator

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	idGenerator IdGenerator
	myId        int
	lastLeader  int
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
	ck.idGenerator.curVal = 1
	ck.myId = globalClerkIdGenerator.make()
	ck.lastLeader = -1
	ck.print("initialized a clerk id %d", ck.myId)
	return ck
}

func (ck *Clerk) print(format string, vars ...interface{}) {
	if Debug {
		s := fmt.Sprintf(format, vars...)
		fmt.Printf("clerk %d | %s\n", ck.myId, s)
	}
}

func (ck *Clerk) sendGet(server int, args *KvCommandArgs, reply *KvCommandReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(server int, args *KvCommandArgs, reply *KvCommandReply) bool {
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) sendSingleCommand(getOrNot bool, server int, args *KvCommandArgs, reply *KvCommandReply, cond *sync.Cond, joinCount *int, leader *int) {
	var ok bool
	if getOrNot {
		//ck.print("sending Get key %s to server %d", args.Key, server)
		ok = ck.sendGet(server, args, reply)
	} else {
		//ck.print("sending PutAppend key %s val %s to server %d", args.Key, args.Value, server)
		ok = ck.sendPutAppend(server, args, reply)
	}
	cond.L.Lock()
	*joinCount++
	if ok {
		if reply.Err == OK || reply.Err == ErrNoKey {
			if *leader != -1 {
				ck.print("leader %d responded already", *leader)
			} else {
				*leader = server
			}
			cond.Broadcast()
		}
	}
	if *joinCount >= len(ck.servers) {
		cond.Broadcast()
	}
	cond.L.Unlock()
}

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
}

const kvRpcWaitMs int = 20

func (ck *Clerk) sendCommandPeriod(getOrNot bool, args *KvCommandArgs) *KvCommandReply {
	if ck.lastLeader != -1 {
		//ck.print("opid %d try first to send to last leader %d", args.OpId, ck.lastLeader)
		reply := KvCommandReply{}
		cond := sync.NewCond(&deadlock.Mutex{})
		joinCount := 0
		leader := -1
		ck.sendSingleCommand(getOrNot, ck.lastLeader, args, &reply, cond, &joinCount, &leader)
		if leader != -1 {
			ck.print("opid %d replied leader unchanged", args.OpId)
			return &reply
		}
	}
	for {
		//ck.print("opid %d try to send to all", args.OpId)
		reply, leader := ck.sendCommandToAll(getOrNot, args)
		if reply != nil {
			ck.lastLeader = leader
			ck.print("opid %d replied updated leader to %d", args.OpId, ck.lastLeader)
			return reply
		}
		time.Sleep(time.Duration(kvRpcWaitMs))
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
		OpId: ck.idGenerator.make(),
		MyId: ck.myId,
	}
	//ck.print("start sending Get key %s opid %d", args.Key, args.OpId)
	reply := ck.sendCommandPeriod(true, &args)
	ck.print("opid %d replied", args.OpId)
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
		OpId:  ck.idGenerator.make(),
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
	ck.sendCommandPeriod(false, &args)
	ck.print("opid %d replied", args.OpId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
