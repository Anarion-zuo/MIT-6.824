package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"6.824/raftservice"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	//servers []*labrpc.ClientEnd
	// Your data here.
	raftClerk *raftservice.RaftClerk
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) print(format string, vars ...interface{}) {
	ck.raftClerk.Print("shardclerk | "+format, vars...)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	//ck.servers = servers
	// Your code here.
	ck.raftClerk = raftservice.MakeRaftClerk(servers)
	return ck
}

func (ck *Clerk) sendRpcCall(server int, args *RpcArgs, reply *RpcReply) {
	ck.raftClerk.SendRpc(server, "ShardCtrler.RpcHandler", args, reply)
}

func (ck *Clerk) Query(num int) Config {
	args := &RpcArgs{}
	// Your code here.
	args.Op = Op{
		Type:             OpQuery,
		QueryConfigIndex: num,
	}
	ck.raftClerk.InitRpcArgs(args)
	var reply *RpcReply
	for {
		reply = ck.raftClerk.SendCommandToLeader(func(server int) raftservice.RpcReply {
			localReply := &RpcReply{}
			ck.sendRpcCall(server, args, localReply)
			return localReply
		}, func(rpcReply raftservice.RpcReply) bool {
			reply := rpcReply.(*RpcReply)
			ck.print("opid %d replied shards %v groups %v", args.Op.OpId, reply.Config.Shards, reply.Config.Groups)
			return true
		}, func() {}).(*RpcReply)
		if reply.Err == OK {
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &RpcArgs{}
	// Your code here.
	args.Op = Op{
		Type:           OpJoin,
		JoinGIDMapping: servers,
	}
	ck.raftClerk.InitRpcArgs(args)
	var reply *RpcReply
	for {
		reply = ck.raftClerk.SendCommandToLeader(func(server int) raftservice.RpcReply {
			localReply := &RpcReply{}
			ck.sendRpcCall(server, args, localReply)
			return localReply
		}, func(rpcReply raftservice.RpcReply) bool {
			ck.print("opid %d replied", args.Op.OpId)
			return true
		}, func() {}).(*RpcReply)
		if reply.Err == OK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &RpcArgs{}
	// Your code here.
	args.Op = Op{
		Type:      OpLeave,
		LeaveGIDs: gids,
	}
	ck.raftClerk.InitRpcArgs(args)
	var reply *RpcReply
	for {
		reply = ck.raftClerk.SendCommandToLeader(func(server int) raftservice.RpcReply {
			localReply := &RpcReply{}
			ck.sendRpcCall(server, args, localReply)
			return localReply
		}, func(rpcReply raftservice.RpcReply) bool {
			ck.print("opid %d replied", args.Op.OpId)
			return true
		}, func() {}).(*RpcReply)
		if reply.Err == OK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &RpcArgs{}
	// Your code here.
	args.Op = Op{
		Type:      OpMove,
		MoveShard: shard,
		MoveGID:   gid,
	}
	ck.raftClerk.InitRpcArgs(args)
	var reply *RpcReply
	for {
		reply = ck.raftClerk.SendCommandToLeader(func(server int) raftservice.RpcReply {
			localReply := &RpcReply{}
			ck.sendRpcCall(server, args, localReply)
			return localReply
		}, func(rpcReply raftservice.RpcReply) bool {
			ck.print("opid %d replied", args.Op.OpId)
			return true
		}, func() {}).(*RpcReply)
		if reply.Err == OK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
