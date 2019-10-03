package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	workerPool := make(chan string, 10)
	go func() {
		for wkName := range mr.registerChannel {
			mr.Workers[wkName] = &WorkerInfo{address: wkName}
			workerPool <- wkName
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		go func(jobRank int) {
			wkName := <-workerPool
			var reply DoJobReply
			for {
				call(wkName, "Worker.DoJob", &DoJobArgs{File: mr.file,
					Operation: JobType(Map), JobNumber: jobRank, NumOtherPhase: mr.nReduce}, &reply)
				if !reply.OK {
					wkName = <-workerPool
				} else {
					workerPool <- wkName
					break
				}
			}
			mr.barrier <- true
		}(i)
	}

	mapFinishCnt := 0
	for range mr.barrier {
		mapFinishCnt++
		if mapFinishCnt == mr.nMap {
			break
		}
	}

	// TO DO
	for i := 0; i < mr.nReduce; i++ {
		go func(jobRank int) {
			wkName := <-workerPool
			var reply DoJobReply
			for {
				call(wkName, "Worker.DoJob", &DoJobArgs{File: mr.file,
					Operation: JobType(Reduce), JobNumber: jobRank, NumOtherPhase: mr.nMap}, &reply)
				if !reply.OK {
					wkName = <-workerPool
				} else {
					workerPool <- wkName
					break
				}
			}
			mr.barrier <- true
		}(i)
	}

	redFinishCnt := 0
	for range mr.barrier {
		redFinishCnt++
		if redFinishCnt == mr.nReduce {
			break
		}
	}
	close(mr.barrier)

	return mr.KillWorkers()
}
