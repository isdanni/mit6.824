package mapreduce

import "fmt"
import "os"
import "log"
import "strconv"
import "encoding/json"
import "sort"
import "container/list"
import "net/rpc"
import "net"
import "bufio"
import "hash/fnv"

// import "os/exec"

// A simple mapreduce library with a sequential implementation.
//
// The application provides an input file f, a Map and Reduce function,
// and the number of nMap and nReduce tasks.
//
// Split() splits the file f in nMap input files:
//    f-0, f-1, ..., f-<nMap-1>
// one for each Map job.
//
// DoMap() runs Map on each map file and produces nReduce files for each
// map file.  Thus, there will be nMap x nReduce files after all map
// jobs are done:
//    f-0-0, ..., f-0-0, f-0-<nReduce-1>, ...,
//    f-<nMap-1>-0, ... f-<nMap-1>-<nReduce-1>.
//
// DoReduce() collects <nReduce> reduce files from each map (f-*-<reduce>),
// and runs Reduce on those files.  This produces <nReduce> result files,
// which Merge() merges into a single output.

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Map and Reduce deal with <key, value> pairs:
type KeyValue struct {
	Key   string
	Value string
}

type MapReduce struct {
	nMap            int    // Number of Map jobs
	nReduce         int    // Number of Reduce jobs
	file            string // Name of input file
	MasterAddress   string
	registerChannel chan string
	DoneChannel     chan bool
	alive           bool
	l               net.Listener
	stats           *list.List

	// Map of registered workers that you need to keep up to date
	Workers map[string]*WorkerInfo

	// add any additional state here
	barrier chan bool
}

func InitMapReduce(nmap int, nreduce int,
	file string, master string) *MapReduce {
	mr := new(MapReduce)
	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = file
	mr.MasterAddress = master
	mr.alive = true
	mr.registerChannel = make(chan string)
	mr.DoneChannel = make(chan bool)

	mr.Workers = make(map[string]*WorkerInfo)
	// initialize any additional state here
	mr.barrier = make(chan bool)
	return mr
}

func MakeMapReduce(nmap int, nreduce int,
	file string, master string) *MapReduce {
	mr := InitMapReduce(nmap, nreduce, file, master)
	mr.StartRegistrationServer()
	go mr.Run()
	return mr
}

func (mr *MapReduce) Register(args *RegisterArgs, res *RegisterReply) error {
	DPrintf("Register: worker %s\n", args.Worker)
	mr.registerChannel <- args.Worker
	res.OK = true
	return nil
}

func (mr *MapReduce) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown: registration server\n")
	mr.alive = false
	mr.l.Close() // causes the Accept to fail
	return nil
}

func (mr *MapReduce) StartRegistrationServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.MasterAddress) // only needed for "unix"
	l, e := net.Listen("unix", mr.MasterAddress)
	if e != nil {
		log.Fatal("RegstrationServer", mr.MasterAddress, " error: ", e)
	}
	mr.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
		for mr.alive {
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				DPrintf("RegistrationServer: accept error", err)
				break
			}
		}
		DPrintf("RegistrationServer: done\n")
	}()
}

// Name of the file that is the input for map job <MapJob>
func MapName(fileName string, MapJob int) string {
	return "mrtmp." + fileName + "-" + strconv.Itoa(MapJob)
}

// Split bytes of input file into nMap splits, but split only on white space
func (mr *MapReduce) Split(fileName string) {
	fmt.Printf("Split %s\n", fileName)
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Split: ", err)
	}
	defer infile.Close()
	fi, err := infile.Stat()
	if err != nil {
		log.Fatal("Split: ", err)
	}
	size := fi.Size()
	nchunk := size / int64(mr.nMap)
	nchunk += 1

	outfile, err := os.Create(MapName(fileName, 0))
	if err != nil {
		log.Fatal("Split: ", err)
	}
	writer := bufio.NewWriter(outfile)
	m := 1
	i := 0

	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		if int64(i) > nchunk*int64(m) {
			writer.Flush()
			outfile.Close()
			outfile, err = os.Create(MapName(fileName, m))
			writer = bufio.NewWriter(outfile)
			m += 1
		}
		line := scanner.Text() + "\n"
		writer.WriteString(line)
		i += len(line)
	}
	writer.Flush()
	outfile.Close()
}

func ReduceName(fileName string, MapJob int, ReduceJob int) string {
	return MapName(fileName, MapJob) + "-" + strconv.Itoa(ReduceJob)
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Read split for job, call Map for that split, and create nreduce
// partitions.
func DoMap(JobNumber int, fileName string,
	nreduce int, Map func(string) *list.List) {
	name := MapName(fileName, JobNumber)
	file, err := os.Open(name)
	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	fi, err := file.Stat()
	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	size := fi.Size()
	fmt.Printf("DoMap: read split %s %d\n", name, size)
	b := make([]byte, size)
	_, err = file.Read(b)
	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	file.Close()
	res := Map(string(b))
	// XXX a bit inefficient. could open r files and run over list once
	for r := 0; r < nreduce; r++ {
		file, err = os.Create(ReduceName(fileName, JobNumber, r))
		if err != nil {
			log.Fatal("DoMap: create ", err)
		}
		enc := json.NewEncoder(file)
		for e := res.Front(); e != nil; e = e.Next() {
			kv := e.Value.(KeyValue)
			if ihash(kv.Key)%uint32(nreduce) == uint32(r) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("DoMap: marshall ", err)
				}
			}
		}
		file.Close()
	}
}

func MergeName(fileName string, ReduceJob int) string {
	return "mrtmp." + fileName + "-res-" + strconv.Itoa(ReduceJob)
}

// Read map outputs for partition job, sort them by key, call reduce for each
// key
func DoReduce(job int, fileName string, nmap int,
	Reduce func(string, *list.List) string) {
	kvs := make(map[string]*list.List)
	for i := 0; i < nmap; i++ {
		name := ReduceName(fileName, i, job)
		fmt.Printf("DoReduce: read %s\n", name)
		file, err := os.Open(name)
		if err != nil {
			log.Fatal("DoReduce: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = list.New()
			}
			kvs[kv.Key].PushBack(kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p := MergeName(fileName, job)
	file, err := os.Create(p)
	if err != nil {
		log.Fatal("DoReduce: create ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := Reduce(k, kvs[k])
		enc.Encode(KeyValue{k, res})
	}
	file.Close()
}

// Merge the results of the reduce jobs
// XXX use merge sort
func (mr *MapReduce) Merge() {
	DPrintf("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := MergeName(mr.file, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create("mrtmp." + mr.file)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}

func RemoveFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

func (mr *MapReduce) CleanupFiles() {
	for i := 0; i < mr.nMap; i++ {
		RemoveFile(MapName(mr.file, i))
		for j := 0; j < mr.nReduce; j++ {
			RemoveFile(ReduceName(mr.file, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		RemoveFile(MergeName(mr.file, i))
	}
	RemoveFile("mrtmp." + mr.file)
}

// Run jobs sequentially.
func RunSingle(nMap int, nReduce int, file string,
	Map func(string) *list.List,
	Reduce func(string, *list.List) string) {
	mr := InitMapReduce(nMap, nReduce, file, "")
	mr.Split(mr.file)
	for i := 0; i < nMap; i++ {
		DoMap(i, mr.file, mr.nReduce, Map)
	}
	for i := 0; i < mr.nReduce; i++ {
		DoReduce(i, mr.file, mr.nMap, Reduce)
	}
	mr.Merge()
}

func (mr *MapReduce) CleanupRegistration() {
	args := &ShutdownArgs{}
	var reply ShutdownReply
	ok := call(mr.MasterAddress, "MapReduce.Shutdown", args, &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.MasterAddress)
	}
	DPrintf("CleanupRegistration: done\n")
}

// Run jobs in parallel, assuming a shared file system
func (mr *MapReduce) Run() {
	fmt.Printf("Run mapreduce job %s %s\n", mr.MasterAddress, mr.file)

	mr.Split(mr.file)
	mr.stats = mr.RunMaster()
	mr.Merge()
	mr.CleanupRegistration()

	fmt.Printf("%s: MapReduce done\n", mr.MasterAddress)

	mr.DoneChannel <- true
}
