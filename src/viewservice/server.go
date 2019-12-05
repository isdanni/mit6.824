package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	isPrimaryAck    bool
	currView        View
	nextView        View
	primaryMissTick int
	backupMissTick  int
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	if args.Me == vs.currView.Primary {
		// ??? Restarted primary treated as dead. !!! The ram data could be lost
		if args.Viewnum == 0 {
			if vs.currView.Backup != "" {
				vs.primaryMissTick = vs.backupMissTick
				vs.backupMissTick = 0
				vs.nextView.Primary = vs.nextView.Backup
				vs.nextView.Backup = ""
				vs.nextView.Viewnum = vs.currView.Viewnum + 1
			} else {
				vs.primaryMissTick = 0
				vs.backupMissTick = 0
				vs.nextView.Primary = ""
				vs.nextView.Backup = ""
				vs.nextView.Viewnum = vs.currView.Viewnum + 1
			}
		}
		vs.primaryMissTick = 0
		if args.Viewnum == vs.currView.Viewnum {
			vs.isPrimaryAck = true
		}
	} else if args.Me == vs.currView.Backup {
		vs.backupMissTick = 0
	} else {
		// if vs.currView.Primary == "", only be assigned when vs.currView.Viewnum == 0
		if vs.currView.Primary == "" && vs.currView.Viewnum == 0 {
			vs.currView.Primary = args.Me
			vs.currView.Viewnum++
			vs.nextView = vs.currView
		} else if vs.currView.Backup == "" && vs.nextView.Backup == "" {
			// if vs.nextView.Backup == "", then vs.currView.Backup == "" definitely
			vs.nextView.Backup = args.Me
			vs.nextView.Viewnum = vs.currView.Viewnum + 1
		} else { // the third alternative
			// nothing
		}
	}
	if vs.isPrimaryAck && vs.currView.Viewnum < vs.nextView.Viewnum {
		vs.currView = vs.nextView
		vs.isPrimaryAck = false
	}
	reply.View = vs.currView
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.View = vs.currView
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	if vs.currView.Primary != "" {
		vs.primaryMissTick++
	}
	if vs.currView.Backup != "" {
		vs.backupMissTick++
	}
	if vs.primaryMissTick == DeadPings {
		vs.primaryMissTick = 0
		if vs.currView.Backup != "" && vs.backupMissTick != DeadPings {
			vs.primaryMissTick = vs.backupMissTick
			vs.backupMissTick = 0
			vs.nextView.Primary = vs.nextView.Backup
			vs.nextView.Backup = ""
			vs.nextView.Viewnum = vs.currView.Viewnum + 1
		} else if vs.currView.Backup != "" && vs.backupMissTick == DeadPings {
			vs.primaryMissTick = 0
			vs.backupMissTick = 0
			vs.nextView.Primary = ""
			vs.nextView.Backup = ""
			vs.nextView.Viewnum = vs.currView.Viewnum + 1
		} else { // vs.currView.Backup == ""
			// primary in view i must have been primary or backup in view i-1
			// so, the nextView.Backup is a invalid candidate of primary before being promoted to currView.Backup.
			vs.primaryMissTick = 0
			vs.nextView.Primary = ""
			vs.nextView.Backup = ""
			vs.nextView.Viewnum = vs.currView.Viewnum + 1
		}
	} else if vs.currView.Backup != "" && vs.backupMissTick == DeadPings {
		vs.backupMissTick = 0
		vs.nextView.Backup = ""
		vs.nextView.Viewnum = vs.currView.Viewnum + 1
	} else {
		// nothing
	}

	// ?, modify in ticking? yes!
	if vs.isPrimaryAck && vs.currView.Viewnum < vs.nextView.Viewnum {
		vs.currView = vs.nextView
		vs.isPrimaryAck = false
	}
	vs.mu.Unlock()

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView = View{0, "", ""}
	vs.nextView = View{0, "", ""}
	vs.primaryMissTick = 0
	vs.backupMissTick = 0
	vs.isPrimaryAck = true

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
