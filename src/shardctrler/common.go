package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type OpType int

const (
	OpJoin  OpType = 1
	OpLeave OpType = 2
	OpMove  OpType = 3
	OpQuery OpType = 4
)

type Op struct {
	// Your data here.
	OpId int
	MyId int

	Type             OpType
	JoinGIDMapping   map[int][]string
	LeaveGIDs        []int
	MoveShard        int
	MoveGID          int
	ConfigNum        int
	QueryConfigIndex int
}

func (o Op) GetOpId() int {
	return o.OpId
}

func (o Op) GetMyId() int {
	return o.MyId
}

func (o Op) CannotRepeat() bool {
	return o.Type != OpQuery
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	//Servers map[int][]string // new GID -> servers mappings
	Op Op
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	//GIDs []int
	Op Op
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	//Shard int
	//GID   int
	Op Op
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	//Num int // desired config number
	Op Op
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type RpcArgs struct {
	Op Op
}

func (r *RpcArgs) GetOpId() int {
	return r.Op.OpId
}

func (r *RpcArgs) SetOpId(i int) {
	r.Op.OpId = i
}

func (r *RpcArgs) GetMyId() int {
	return r.Op.MyId
}

func (r *RpcArgs) SetMyId(i int) {
	r.Op.MyId = i
}

type RpcReply struct {
	WrongLeader bool
	Err         Err
	Config      Config

	CommitIntex int
	Term        int
}

func (r *RpcReply) GetCommitIndex() int {
	return r.CommitIntex
}

func (r *RpcReply) SetCommitIndex(i int) {
	r.CommitIntex = i
}

func (r *RpcReply) GetTerm() int {
	return r.Term
}

func (r *RpcReply) SetTerm(i int) {
	r.Term = i
}

func (r *RpcReply) GetIsLeader() bool {
	return !r.WrongLeader
}

func (r *RpcReply) SetIsLeader(b bool) {
	r.WrongLeader = !b
}
