package raftservice

//type RpcArgs struct {
//	OpId int
//	MyId int
//}

type RpcArgs interface {
	GetOpId() int
	SetOpId(int)
	GetMyId() int
	SetMyId(int)
}

type RpcReply interface {
	GetCommitIndex() int
	SetCommitIndex(int)
	GetTerm() int
	SetTerm(int)
	GetIsLeader() bool
	SetIsLeader(bool)
}
