package kvpaxos

const (
	OK           = "OK"
	ErrNoKey     = "ErrNoKey"
	ErrPending   = "ErrPending"
	ErrForgotten = "ErrForgotten"
)

const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Id    int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Id  int64
}

type GetReply struct {
	Err   Err
	Value string
}
