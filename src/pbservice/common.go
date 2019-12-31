package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const (
	Put    = "Put"
	Append = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string
	Sender   string
	IsClient bool  // from a normal client or server
	Seq      int64 // dedup the same client request
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type BackupArgs struct {
	Dataset         map[string]string
	ProcessedSeqSet map[int64]bool
	Sender          string
}

type BackupReply struct {
	Err Err
}

// Your RPC definitions here.
