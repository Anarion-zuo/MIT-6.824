package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrNotCommitted = "ErrNotCommitted"

	PutAppendCommand int = 501
	GetCommand       int = 503
)

type Err string

// Put or Append
type KvCommandArgs struct {
	OpId int
	MyId int

	Key       string
	Value     string
	Op        int // "Put" or "Append"
	Overwrite bool
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func (k *KvCommandArgs) GetOpId() int {
	return k.OpId
}

func (k *KvCommandArgs) SetOpId(i int) {
	k.OpId = i
}

func (k *KvCommandArgs) GetMyId() int {
	return k.MyId
}

func (k *KvCommandArgs) SetMyId(i int) {
	k.MyId = i
}

type KvCommandReply struct {
	CommitIndex int
	Term        int
	IsLeader    bool

	Err   Err
	Value string
}

func (k KvCommandReply) GetCommitIndex() int {
	return k.CommitIndex
}

func (k KvCommandReply) SetCommitIndex(i int) {
	k.CommitIndex = i
}

func (k KvCommandReply) GetTerm() int {
	return k.Term
}

func (k KvCommandReply) SetTerm(i int) {
	k.Term = i
}

func (k KvCommandReply) GetIsLeader() bool {
	return k.IsLeader
}

func (k KvCommandReply) SetIsLeader(i bool) {
	k.IsLeader = i
}
