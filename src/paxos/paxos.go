package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	proposerMgr *ProposerManager
	acceptorMgr *AcceptorManager

	chosen_value map[int]interface{}

	doneSeqs     []int // non-local, except doneSeqs[me]
	minDoneSeq   int   // the minimal in doneSeqs
	minDoneIndex int   // the index of the minimal in doneSeqs
}

type ProposerManager struct {
	px             *Paxos
	mu             sync.Mutex
	peers          []string
	me             int // index of the proposers
	proposers      map[int]*Proposer
	seq_max        int
	seq_chosen_max int
}

type AcceptorManager struct {
	mu        sync.Mutex
	acceptors map[int]*Acceptor
	seq_max   int
}

type Acceptor struct {
	mu sync.Mutex
	// init: -1, -1, ""
	n_p int
	n_a int
	v_a interface{}
}

type Proposer struct {
	mgr           *ProposerManager
	seq           int
	propose_value interface{}
	isDead        bool
}

type PrepareArgs struct {
	Seq int
	N   int
}

type PrepareReply struct {
	N    int // for choosing next proposing number
	N_a  int
	V_a  interface{}
	Succ bool
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptReply struct {
	N    int // for choosing next proposing number
	Succ bool
}

type DecideArgs struct {
	Seq int
	V   interface{}
}

type DecideReply bool

type SeqArgs struct {
	Sender int
	Seq    int
}

type SeqReply bool

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func (proposerMgr *ProposerManager) RunProposer(seq int, v interface{}) {
	proposerMgr.mu.Lock()
	defer proposerMgr.mu.Unlock()
	if _, ok := proposerMgr.proposers[seq]; !ok {
		if seq > proposerMgr.seq_max {
			proposerMgr.seq_max = seq
		}
		prop := &Proposer{mgr: proposerMgr, seq: seq, propose_value: v, isDead: false}
		proposerMgr.proposers[seq] = prop
		go func() {
			prop.Propose()
		}()
	}
}

func (acceptorMgr *AcceptorManager) GetInstance(seq int) *Acceptor {
	acceptorMgr.mu.Lock()
	defer acceptorMgr.mu.Unlock()
	acceptor, ok := acceptorMgr.acceptors[seq]
	if !ok {
		if seq > acceptorMgr.seq_max {
			acceptorMgr.seq_max = seq
		}
		acceptor = &Acceptor{n_p: -1, n_a: -1, v_a: nil}
		acceptorMgr.acceptors[seq] = acceptor
	}
	return acceptor
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	acceptor := px.acceptorMgr.GetInstance(args.Seq)
	acceptor.mu.Lock()
	defer acceptor.mu.Unlock()
	if args.N > acceptor.n_p {
		reply.Succ = true
		acceptor.n_p = args.N
		reply.N = args.N
		reply.N_a = acceptor.n_a
		reply.V_a = acceptor.v_a
	} else {
		reply.Succ = false
		reply.N = acceptor.n_p
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	acceptor := px.acceptorMgr.GetInstance(args.Seq)
	acceptor.mu.Lock()
	defer acceptor.mu.Unlock()
	if args.N >= acceptor.n_p {
		reply.Succ = true
		acceptor.n_p = args.N
		acceptor.n_a = args.N
		acceptor.v_a = args.V
		reply.N = args.N
	} else {
		reply.Succ = false
		reply.N = acceptor.n_p
	}
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// assertion: it must be an idempotent operation
	px.chosen_value[args.Seq] = args.V
	*reply = DecideReply(true)
	return nil
}

// loop for getting the chosen value, except being dead
func (proposer *Proposer) Propose() {
	peers_num := len(proposer.mgr.peers)
	majority_num := peers_num/2 + 1

	propose_num := proposer.mgr.me
	for !proposer.isDead {
		next_propose_num := propose_num
		// prepare request
		prepareReplies := make(chan PrepareReply, peers_num)
		prepareBarrier := make(chan bool)
		for me, peer := range proposer.mgr.peers {
			go func(me int, peer string) {
				args := &PrepareArgs{Seq: proposer.seq, N: propose_num}
				var reply PrepareReply

				// avoid the situation that local rpc is fragile, however the acceptor should prepare value that itself issued.
				succ := true
				if me != proposer.mgr.me {
					succ = call(peer, "Paxos.Prepare", args, &reply)
				} else {
					proposer.mgr.px.Prepare(args, &reply)
				}
				prepareBarrier <- true
				if succ {
					if reply.Succ {
						prepareReplies <- reply
					} else if reply.N > next_propose_num {
						// TODO atomic
						next_propose_num = reply.N
					}
				}
			}(me, peer)
		}

		// barrier
		for i := 0; i < peers_num; i++ {
			<-prepareBarrier
		}

		if len(prepareReplies) >= majority_num {
			var accepted_value interface{} = nil
			accepted_propose_num := -1
			replies_num := len(prepareReplies)
			for i := 0; i < replies_num; i++ {
				r := <-prepareReplies
				if r.N_a > accepted_propose_num {
					accepted_propose_num = r.N_a
					accepted_value = r.V_a
				}
			}

			// accept request
			acceptReplies := make(chan AcceptReply, peers_num)
			acceptBarrier := make(chan bool)
			var accept_req_value interface{}

			if accepted_value != nil {
				// accepting has happened, use the accepted value
				accept_req_value = accepted_value
			} else {
				// no accept, use own propose value
				accept_req_value = proposer.propose_value
			}
			for me, peer := range proposer.mgr.peers {
				go func(me int, peer string) {
					args := &AcceptArgs{Seq: proposer.seq, N: propose_num, V: accept_req_value}
					var reply AcceptReply
					// same reason
					succ := true
					if me != proposer.mgr.me {
						succ = call(peer, "Paxos.Accept", args, &reply)
					} else {
						proposer.mgr.px.Accept(args, &reply)
					}
					acceptBarrier <- true
					if succ {
						if reply.Succ {
							acceptReplies <- reply
						} else if reply.N > next_propose_num {
							next_propose_num = reply.N
						}
					}
				}(me, peer)
			}

			// barrier
			for i := 0; i < peers_num; i++ {
				<-acceptBarrier
			}

			// Decide procedure is broadcast the learn value
			if len(acceptReplies) >= majority_num {
				// be sure to get a chosen value
				for me, peer := range proposer.mgr.peers {
					go func(me int, peer string) {
						args := &DecideArgs{Seq: proposer.seq, V: accept_req_value}
						var reply DecideReply
						// same reason
						if me != proposer.mgr.me {
							call(peer, "Paxos.Decide", args, &reply)
							/* For unreliable rpc and controlling resource, it's better
							use the following. It's not necessary, others can lanunch
							the `start` procedure.
							succ := false
							for !succ && !proposer.isDead {
								succ = call(peer, "Paxos.Decide", args, &reply)
								time.Sleep(time.Second)
							}
							*/
						} else {
							proposer.mgr.px.Decide(args, &reply)
						}
					}(me, peer)
				}
				break
			} // end if accept
		} // end if prepare

		try_num := next_propose_num/peers_num*peers_num + proposer.mgr.me
		if try_num > next_propose_num {
			next_propose_num = try_num
		} else {
			next_propose_num = try_num + peers_num
		}
		// assertion: next_propose_num become bigger
		if next_propose_num <= propose_num || next_propose_num%peers_num != proposer.mgr.me {
			log.Fatalln("unexpected error!!!")
		}
		propose_num = next_propose_num

		// TODO
		// sleep for avoiding thrashing of proposing
		time.Sleep(50 * time.Millisecond)
	}

}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	// TODO optimize, if we know the history log, just avoid the
	// paxos proposing process.

	// ignore the instance whose seq isn't more than minDoneSeq
	if seq <= px.minDoneSeq {
		return
	}

	px.proposerMgr.RunProposer(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	for me, peer := range px.peers {
		go func(me int, peer string) {
			args := &SeqArgs{Seq: seq, Sender: px.me}
			var reply SeqReply
			if me != px.me {
				call(peer, "Paxos.UpdateDoneSeqs", args, &reply)
			} else {
				// the same reason using local invocation instead of RPC
				px.UpdateDoneSeqs(args, &reply)
			}
		}(me, peer)
	}
}

func (px *Paxos) UpdateDoneSeqs(args *SeqArgs, reply *SeqReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Seq > px.doneSeqs[args.Sender] {
		px.doneSeqs[args.Sender] = args.Seq
		// minimal changes only when the former one become bigger
		if args.Sender == px.minDoneIndex {
			px.minDoneSeq = args.Seq
			for index, seq := range px.doneSeqs {
				if seq < px.minDoneSeq {
					px.minDoneSeq = seq
					px.minDoneIndex = index
				}
			}

			// release instance

			px.proposerMgr.mu.Lock()
			for s, prop := range px.proposerMgr.proposers {
				if s <= px.minDoneSeq {
					prop.isDead = true
					delete(px.proposerMgr.proposers, s)
				}
			}
			px.proposerMgr.mu.Unlock()

			for s, _ := range px.chosen_value {
				if s <= px.minDoneSeq {
					delete(px.chosen_value, s)
				}
			}

			px.acceptorMgr.mu.Lock()
			for s, _ := range px.acceptorMgr.acceptors {
				if s <= px.minDoneSeq {
					delete(px.acceptorMgr.acceptors, s)
				}
			}
			px.acceptorMgr.mu.Unlock()

		}
	}
	return nil
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	a, b := px.acceptorMgr.seq_max, px.proposerMgr.seq_max
	if a > b {
		return a
	} else {
		return b
	}
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.minDoneSeq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if value, ok := px.chosen_value[seq]; seq <= px.minDoneSeq {
		return Forgotten, nil
	} else if ok {
		return Decided, value
	} else {
		return Pending, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.chosen_value = make(map[int]interface{})

	px.doneSeqs = make([]int, len(px.peers))
	for i := 0; i < len(px.peers); i++ {
		px.doneSeqs[i] = -1
	}
	px.minDoneSeq = -1
	px.minDoneIndex = 0

	px.proposerMgr = &ProposerManager{peers: peers, me: me, proposers: make(map[int]*Proposer), seq_max: -1, seq_chosen_max: -1, px: px}
	px.acceptorMgr = &AcceptorManager{acceptors: make(map[int]*Acceptor), seq_max: -1}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
		rpcs.Register(px.acceptorMgr)
		rpcs.Register(px.proposerMgr)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)
		rpcs.Register(px.acceptorMgr)
		rpcs.Register(px.proposerMgr)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
