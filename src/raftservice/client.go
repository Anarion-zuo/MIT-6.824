package raftservice

import (
	"6.824/labrpc"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"time"
)

var globalClerkIdGenerator IdGenerator

type RaftClerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	opIdGenerator IdGenerator
	myId          int
	lastLeader    int
	// lock on lastLeader
	mu deadlock.Mutex

	printFlag bool
}

func (ck *RaftClerk) Print(format string, vars ...interface{}) {
	if ck.printFlag {
		s := fmt.Sprintf(format, vars...)
		fmt.Printf("rfclerk %d | %s\n", ck.myId, s)
	}
}

func (ck *RaftClerk) Id() int {
	return ck.myId
}

func (ck *RaftClerk) InitRpcArgs(args RpcArgs) {
	args.SetOpId(ck.opIdGenerator.make())
	args.SetMyId(ck.myId)
}

func (ck *RaftClerk) SendRpc(server int, name string, args RpcArgs, reply RpcReply) RpcReply {
	ok := ck.servers[server].Call(name, args, reply)
	if !ok {
		return nil
	}
	return reply
}

// reuturns whether server is leader and reachable in the operation
func (ck *RaftClerk) sendSingleCommand(server int, sendRpcFn func(server int) RpcReply, serverReachableFn func(reply RpcReply) bool, serverUnreachableFn func()) RpcReply {
	reply := sendRpcFn(server)
	if reply != nil {
		if reply.GetIsLeader() {
			ok := serverReachableFn(reply)
			if !ok {
				return nil
			}
			return reply
		} else {
			return nil
		}
	}
	serverUnreachableFn()
	ck.Print("server %d unreachable", server)
	return nil
}

const kvRpcWaitMs int = 50

type _AsyncReply struct {
	reply  RpcReply
	server int
}

func (ck *RaftClerk) sendCommandToAll(sendRpcFn func(server int) RpcReply, serverReachableFn func(reply RpcReply) bool, serverUnreachableFn func()) RpcReply {
	replyCh := make(chan _AsyncReply)
	for i := 0; i < len(ck.servers); i++ {
		i := i
		go func() {
			//ck.Print("probing server %d", i)
			reply := ck.sendSingleCommand(i, sendRpcFn, serverReachableFn, serverUnreachableFn)
			if reply != nil {
				replyCh <- _AsyncReply{
					reply:  reply,
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
	var reply RpcReply = nil
	leader := -1
	for i := 0; i < len(ck.servers); i++ {
		newReply := <-replyCh
		//ck.Print("%d replied isLeader %t", newReply.server, newReply.reply != nil)
		if newReply.reply != nil {
			if reply != nil {
				ck.Print("%d claims to be a leader, but already found a leader %d", newReply.server, leader)
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

func (ck *RaftClerk) SendCommandToLeader(sendRpcFn func(server int) RpcReply, leaderServerReachableFn func(reply RpcReply) bool, serverUnreachableFn func()) RpcReply {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.lastLeader != -1 {
		ck.Print("sending to previous leader %d", ck.lastLeader)
		reply := ck.sendSingleCommand(ck.lastLeader, sendRpcFn, leaderServerReachableFn, serverUnreachableFn)
		if reply != nil {
			ck.Print("leader %d unchanged", ck.lastLeader)
			return reply
		}
	}
	for {
		ck.Print("probing for a new leader")
		reply := ck.sendCommandToAll(sendRpcFn, leaderServerReachableFn, serverUnreachableFn)
		if reply != nil {
			ck.Print("found new leader %d", ck.lastLeader)
			return reply
		}
		time.Sleep(time.Duration(kvRpcWaitMs) * time.Millisecond)
	}
}

func MakeRaftClerk(servers []*labrpc.ClientEnd) *RaftClerk {
	ck := new(RaftClerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.opIdGenerator.curVal = 1
	ck.myId = globalClerkIdGenerator.make()
	ck.mu.Lock()
	ck.lastLeader = -1
	ck.mu.Unlock()
	ck.Print("initialized a clerk id %d", ck.myId)
	ck.printFlag = true
	return ck
}
