package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpName string
	Key    string
	Value  string
	Id     int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	content map[string]string
	seq     int // seq for next req
	history map[int64]bool
}

func (kv *KVPaxos) apply(op *Op) {
	switch op.OpName {
	case Put:
		kv.content[op.Key] = op.Value
	case Append:
		kv.content[op.Key] += op.Value
	default:
		// nothing
	}
	// Inject the GET value into the history,
	// the Write op can be recorded without value for dedup.
	kv.history[op.Id] = true
	return
}

// Try to decide the op in one of the paxos instance
// increase the seq until decide the op that we want,
// and apply the chosen value to the kv store.
func (kv *KVPaxos) TryDecide(op Op) (Err, string) {
	// TODO concurrency optimization
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.history[op.Id]; ok {
		if op.OpName == Get {
			return OK, kv.content[op.Key]
		} else {
			return OK, ""
		}
	}
	chosen := false
	for !chosen {
		timeout := 0 * time.Millisecond
		sleep_interval := 10 * time.Millisecond
		kv.px.Start(kv.seq, op)
	INNER:
		for {
			fate, v := kv.px.Status(kv.seq)
			switch fate {
			case paxos.Decided:
				{
					_op := v.(Op)
					kv.px.Done(kv.seq)
					kv.apply(&_op)
					kv.seq++
					if _op.Id == op.Id {
						if _op.OpName == Get {
							if v, ok := kv.content[op.Key]; ok {
								return OK, v
							} else {
								return ErrNoKey, ""
							}
						}
						// for put/append operation
						chosen = true
					}
					break INNER
				}
			case paxos.Pending:
				{
					if timeout > 10*time.Second {
						return ErrPending, ""
					} else {
						time.Sleep(sleep_interval)
						timeout += sleep_interval
						sleep_interval *= 2
					}
				}
			default:
				// Forgotten, do nothing for impossibility
				return ErrForgotten, ""
			}
		}
	}
	return OK, ""
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := Op{OpName: Get, Key: args.Key, Value: "", Id: args.Id}
	reply.Err, reply.Value = kv.TryDecide(op)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	op := Op{OpName: args.Op, Key: args.Key, Value: args.Value, Id: args.Id}
	reply.Err, _ = kv.TryDecide(op)
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.content = make(map[string]string)
	kv.history = make(map[int64]bool)
	kv.seq = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()
	return kv
}
