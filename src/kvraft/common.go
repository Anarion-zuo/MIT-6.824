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
	Key       string
	Value     string
	Op        int // "Put" or "Append"
	OpId      int
	Overwrite bool
	MyId      int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KvCommandReply struct {
	Err   Err
	Value string
}
