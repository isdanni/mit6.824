package shardmaster

import crand "crypto/rand"
import "errors"
import "fmt"
import "log"
import "math/big"
import "net"
import "net/rpc"
import "time"
import "encoding/gob"
import "math/rand"
import "os"
import "paxos"
import "sort"
import "sync"
import "sync/atomic"
import "syscall"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	// TODO delete
	currGroupShardsMap map[int64][]int // group GID -> shards
	mapMu              sync.Mutex
	// currGroupShardsMap *sync.Map // group GID -> shards
	seq      int
	assigned bool // whether there exists a valid group
}

type GroupShardsNumSlice []GroupShardsNum

type GroupShardsNum struct {
	num int
	GID int64
}

type ShardGid struct {
	shard int
	GID   int64
}

func (s GroupShardsNumSlice) Len() int           { return len(s) }
func (s GroupShardsNumSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s GroupShardsNumSlice) Less(i, j int) bool { return s[i].num < s[j].num }

type Op struct {
	// Your data here.
	OpName  string
	GID     int64
	Num     int
	Shard   int
	Servers []string
	RID     int64
}

// TODO user-defined
const (
	OP_JOIN  = "join"
	OP_LEAVE = "leave"
	OP_MOVE  = "move"
	OP_QUERY = "query"
)

// TODO user-defined
func (sm *ShardMaster) join(op *Op) {
	lastCfg := sm.configs[len(sm.configs)-1]
	// the group exists
	if _, ok := lastCfg.Groups[op.GID]; ok {
		return
	}

	newCfg := copyCfg(lastCfg)
	defer func() { sm.configs = append(sm.configs, newCfg) }()
	newCfg.Groups[op.GID] = op.Servers

	// there is no groups before
	if !sm.assigned {
		shards := make([]int, NShards)
		for i := 0; i < NShards; i++ {
			newCfg.Shards[i] = op.GID
			shards[i] = i
		}
		sm.currGroupShardsMap[op.GID] = shards
		sm.assigned = true
		return
	}

	prevGroupsNum := len(sm.currGroupShardsMap)
	newGroupsNum := prevGroupsNum + 1

	// add a blank element
	var lastShardsNum GroupShardsNumSlice = make([]GroupShardsNum, 0, newGroupsNum)
	lastShardsNum = append(lastShardsNum, GroupShardsNum{num: 0, GID: 0})
	for gid, shards := range sm.currGroupShardsMap {
		lastShardsNum = append(lastShardsNum, GroupShardsNum{num: len(shards), GID: gid})
	}
	sort.Sort(lastShardsNum)

	newShardsNum := getGroupsSize(NShards, newGroupsNum)

	// add the joined group
	sm.currGroupShardsMap[op.GID] = []int{}

	shardsChan := make(chan int, NShards)
	var wg sync.WaitGroup
	wg.Add(newGroupsNum)
	go func() {
		// move in <diff> shards to the joined group
		for j := 0; j < newShardsNum[0]; j++ {
			shard := <-shardsChan
			sm.mapMu.Lock()
			sm.currGroupShardsMap[op.GID] = append(sm.currGroupShardsMap[op.GID], shard)
			sm.mapMu.Unlock()
			newCfg.Shards[shard] = op.GID
		}
		wg.Done()
	}()
	for i := 1; i < newGroupsNum; i++ {
		aGID := lastShardsNum[i].GID
		diff := newShardsNum[i] - lastShardsNum[i].num
		if diff < 0 {
			go func() {
				// move out <diff> shards in group of aGID
				sm.mapMu.Lock()
				outSlice := sm.currGroupShardsMap[aGID][:-diff]
				for _, shard := range outSlice {
					shardsChan <- shard
				}
				sm.currGroupShardsMap[aGID] = sm.currGroupShardsMap[aGID][-diff:]
				sm.mapMu.Unlock()
				wg.Done()
			}()
		} else if diff > 0 {
			go func() {
				// move in <diff> shards to group of aGID
				for j := 0; j < diff; j++ {
					shard := <-shardsChan
					sm.mapMu.Lock()
					sm.currGroupShardsMap[aGID] = append(sm.currGroupShardsMap[aGID], shard)
					sm.mapMu.Unlock()
					newCfg.Shards[shard] = aGID
				}
				wg.Done()
			}()
		} else {
			wg.Done()
		}
	}
	wg.Wait()
}

func (sm *ShardMaster) leave(op *Op) {
	lastCfg := sm.configs[len(sm.configs)-1]
	if _, ok := lastCfg.Groups[op.GID]; !ok {
		// doesn't exist
		return
	}

	newCfg := copyCfg(lastCfg)

	prevGroupsNum := len(sm.currGroupShardsMap)
	newGroupsNum := prevGroupsNum - 1
	defer func() {
		delete(sm.currGroupShardsMap, op.GID)
		delete(newCfg.Groups, op.GID)
		sm.configs = append(sm.configs, newCfg)
	}()

	// there is no groups after leaving
	if newGroupsNum == 0 {
		for i := 0; i < NShards; i++ {
			newCfg.Shards[i] = 0
		}
		sm.assigned = false
		return
	}

	var lastShardsNum GroupShardsNumSlice = make([]GroupShardsNum, 0, newGroupsNum)
	for gid, shards := range sm.currGroupShardsMap {
		if gid != op.GID {
			lastShardsNum = append(lastShardsNum, GroupShardsNum{num: len(shards), GID: gid})
		}
	}
	sort.Sort(lastShardsNum)

	newShardsNum := getGroupsSize(NShards, newGroupsNum)

	shardsChan := make(chan int, NShards)
	var wg sync.WaitGroup
	wg.Add(prevGroupsNum)
	go func() {
		// move out all shards in group of op.GID
		sm.mapMu.Lock()
		for _, shard := range sm.currGroupShardsMap[op.GID] {
			shardsChan <- shard
		}
		sm.mapMu.Unlock()
		wg.Done()
	}()
	for i := 0; i < newGroupsNum; i++ {
		aGID := lastShardsNum[i].GID
		diff := newShardsNum[i] - lastShardsNum[i].num
		if diff < 0 {
			go func() {
				// move out <diff> shards in group of aGID
				sm.mapMu.Lock()
				outSlice := sm.currGroupShardsMap[aGID][:-diff]
				for _, shard := range outSlice {
					shardsChan <- shard
				}
				sm.currGroupShardsMap[aGID] = sm.currGroupShardsMap[aGID][-diff:]
				sm.mapMu.Unlock()
				wg.Done()
			}()
		} else if diff > 0 {
			go func() {
				// move in <diff> shards to group of aGID
				for j := 0; j < diff; j++ {
					shard := <-shardsChan
					sm.mapMu.Lock()
					sm.currGroupShardsMap[aGID] = append(sm.currGroupShardsMap[aGID], shard)
					sm.mapMu.Unlock()
					newCfg.Shards[shard] = aGID
				}
				wg.Done()
			}()
		} else {
			wg.Done()
		}
	}
	wg.Wait()
}

func (sm *ShardMaster) move(op *Op) {
	lastCfg := sm.configs[len(sm.configs)-1]
	if _, ok := lastCfg.Groups[op.GID]; !ok {
		// doesn't exist
		return
	}

	if lastCfg.Shards[op.Shard] == op.GID {
		// keep status
		return
	}

	newCfg := copyCfg(lastCfg)
	defer func() { sm.configs = append(sm.configs, newCfg) }()

	prevGID := newCfg.Shards[op.Shard]
	prevShards := sm.currGroupShardsMap[prevGID]
	newShards := make([]int, 0, len(prevShards)-1)
	for _, s := range prevShards {
		if s != op.Shard {
			newShards = append(newShards, s)
		}
	}
	sm.currGroupShardsMap[prevGID] = newShards

	newCfg.Shards[op.Shard] = op.GID
	sm.currGroupShardsMap[op.GID] = append(sm.currGroupShardsMap[op.GID], op.Shard)
	return
}

func (sm *ShardMaster) query(op *Op) interface{} {
	if op.Num == -1 {
		return sm.configs[len(sm.configs)-1]
	} else if op.Num < 0 || op.Num >= len(sm.configs) {
		return nil
	} else {
		return sm.configs[op.Num]
	}
}

// return the size of groups in the ascending order
func getGroupsSize(nShards, groupsNum int) []int {
	quotient := NShards / groupsNum
	remainder := NShards - quotient*groupsNum
	groupsSize := make([]int, groupsNum)
	for i := 0; i < groupsNum; i++ {
		if i < groupsNum-remainder {
			groupsSize[i] = quotient
		} else {
			groupsSize[i] = quotient + 1
		}
	}
	return groupsSize
}

// copy the previous config and increment the Num
func copyCfg(lastCfg Config) Config {
	newGroups := make(map[int64][]string)
	for gid, ss := range lastCfg.Groups {
		newGroups[gid] = ss
	}
	newCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: newGroups}
	return newCfg
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) apply(op *Op) {
	switch op.OpName {
	case OP_JOIN:
		sm.join(op)
	case OP_LEAVE:
		sm.leave(op)
	case OP_MOVE:
		sm.move(op)
	case OP_QUERY:
		sm.query(op)
	}
}

// TODO user-defined
// Try to decide the op in one of the paxos instance
// increase the seq until decide the op that we want,
// and apply the chosen value to the kv store.
func (sm *ShardMaster) TryDecide(op Op) error {
	op.RID = nrand()
	for {
		timeout := 0 * time.Millisecond
		sleep_interval := 10 * time.Millisecond
		sm.px.Start(sm.seq, op)
	INNER:
		for {
			fate, v := sm.px.Status(sm.seq)
			switch fate {
			case paxos.Decided:
				{
					_op := v.(Op)
					sm.px.Done(sm.seq)
					sm.seq++
					if _op.RID == op.RID {
						return nil
					} else {
						// apply the previous ops
						sm.apply(&_op)
					}
					break INNER
				}
			case paxos.Pending:
				{
					if timeout > 10*time.Second {
						return errors.New("timeout")
					} else {
						time.Sleep(sleep_interval)
						timeout += sleep_interval
						sleep_interval *= 2
					}
				}
			default:
				// Forgotten, do nothing for impossibility
				return errors.New("paxos forgotten")
			}
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{OpName: OP_JOIN, GID: args.GID, Servers: args.Servers}
	var err error
	if err = sm.TryDecide(op); err == nil {
		sm.join(&op)
	}
	return err

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{OpName: OP_LEAVE, GID: args.GID}
	var err error
	if err = sm.TryDecide(op); err == nil {
		sm.leave(&op)
	}
	return err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{OpName: OP_MOVE, GID: args.GID, Shard: args.Shard}
	var err error
	if err = sm.TryDecide(op); err == nil {
		sm.move(&op)
	}
	return err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{OpName: OP_QUERY, Num: args.Num}
	var err error
	if err = sm.TryDecide(op); err == nil {
		reply.Config = sm.query(&op).(Config)
	}
	return err
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sm.configs[0].Shards[i] = 0
	}
	sm.configs[0].Groups = map[int64][]string{}
	sm.currGroupShardsMap = make(map[int64][]int)
	sm.assigned = false
	sm.seq = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
