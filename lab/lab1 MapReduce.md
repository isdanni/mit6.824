# 6.824 Lab 1: MapReduce

In this lab you'll build a **MapReduce library** as an introduction to programming in Go and to building fault tolerant distributed systems. In the first part you will write a simple MapReduce program. In the second part you will write a Master that hands out tasks to MapReduce workers, and handles failures of workers. The interface to the library and the approach to fault tolerance is similar to the one described in the original [MapReduce paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

### Software 

You'll implement this lab (and all the labs) in Go. The Go web site contains lots of tutorial information which you may want to look at. We will grade your labs using Go version 1.9; you should use 1.9 too, though we don't know of any problems with other versions.

The labs are designed to run on **Athena Linux machines** with x86 or x86_64 architecture; `uname -a` should mention `i386 GNU/Linux` or `i686 GNU/Linux` or `x86_64 GNU/Linux`. You can log into a public Athena host with `ssh athena.dialup.mit.edu`. You may get lucky and find that the labs work in other environments, for example on some laptop Linux or OSX installations.

We supply you with parts of a MapReduce implementation that supports both **distributed** and **non-distributed** operation (just the boring bits). You'll fetch the initial lab software with git (a version control system). To learn more about git, look at the Pro Git book or the git user's manual, or, if you are already familiar with other version control systems, you may find this CS-oriented overview of git useful.

These Athena commands will give you access to git and Go:

```shell
athena$ add git
athena$ setup ggo_v1.9
```

The URL for the course git repository is [git://g.csail.mit.edu/6.824-golabs-2018](git://g.csail.mit.edu/6.824-golabs-2018). To install the files in your directory, you need to clone the course repository, by running the commands below.

```shell
$ git clone git://g.csail.mit.edu/6.824-golabs-2018 6.824
$ cd 6.824
$ ls
Makefile src
```

Git allows you to keep track of the changes you make to the code. For example, if you want to checkpoint your progress, you can commit your changes by running:

`$ git commit -am 'partial solution to lab 1'`

The Map/Reduce implementation we give you has support for **two modes of operation**, **sequential** and **distributed**. In the former, the map and reduce tasks are executed one at a time: first, the first map task is executed to completion, then the second, then the third, etc. When all the map tasks have finished, the first reduce task is run, then the second, etc. This mode, while not very fast, is useful for debugging. The distributed mode runs many worker threads that first **execute map tasks in parallel, and then reduce tasks**. This is much faster, but also harder to implement and debug.

### Preamble: Getting familiar with the source

The mapreduce package provides a simple Map/Reduce library (in the mapreduce directory). Applications should normally call `Distributed()` [located in `master.go`] to start a job, but may instead call `Sequential()` [also in master.go] to get a sequential execution for debugging.

The code executes a job as follows:

1. The application provides a number of input files, a map function, a reduce function, and the number of reduce tasks (`nReduce`).

2. A master is created with this knowledge. It starts an RPC server (see `master_rpc.go`), and waits for workers to register (using the RPC call `Register()` [defined in master.go]). As tasks become available (in steps 4 and 5), `schedule()` [schedule.go] decides how to assign those tasks to workers, and how to handle worker failures.

3. The master considers each input file to be one map task, and calls `doMap()` [common_map.go] **at least once for each map task**. It does so either directly (when using `Sequential()`) or by issuing the `DoTask` RPC to a worker [worker.go]. Each call to doMap() reads the appropriate file, calls the map function on that file's contents, and writes the resulting key/value pairs to `nReduce` intermediate files. `doMap()` hashes each key to pick the intermediate file and thus the reduce task that will process the key. There will be `nMap` x `nReduce` files after all map tasks are done. Each file name contains **a prefix**, the **map task number**, and the **reduce task number**. If there are two map tasks and three reduce tasks, the map tasks will create these six intermediate files:

```
mrtmp.xxx-0-0
mrtmp.xxx-0-1
mrtmp.xxx-0-2
mrtmp.xxx-1-0
mrtmp.xxx-1-1
mrtmp.xxx-1-2
```

**Each worker must be able to read files written by any other worker, as well as the input files**. Real deployments use distributed storage systems such as `GFS` to allow this access even though workers run on different machines. In this lab you'll run all the workers on the same machine, and use the local file system.

4. The master next calls `doReduce()` [common_reduce.go] at least once for each reduce task. As with doMap(), it does so either directly or through a worker. The doReduce() for reduce task r collects the r'th intermediate file from each map task, and calls the reduce function for each key that appears in those files. The reduce tasks produce nReduce result files.

5. The master calls `mr.merge()` [master_splitmerge.go], which merges all the nReduce files produced by the previous step into a single output.

6. The master sends a Shutdown RPC to each of its workers, and then shuts down its own RPC server.


> **Note**: Over the course of the following exercises, you will have to write/modify `doMap`, `doReduce`, and `schedule` yourself. These are located in `common_map.go`, `common_reduce.go`, and `schedule.go` respectively. You will also have to write the map and reduce functions in ../`main/wc.go`.

You should not need to modify any other files, but reading them might be useful in order to understand how the other methods fit into the overall architecture of the system.

### Part I: Map/Reduce input and output

The Map/Reduce implementation you are given is missing some pieces. Before you can write your first Map/Reduce function pair, you will need to fix the sequential implementation. In particular, the code we give you is missing two crucial pieces: the function that **divides up the output of a map task**, and the function that **gathers all the inputs for a reduce task**. These tasks are carried out by the `doMap()` function in `common_map.go`, and the `doReduce()` function in `common_reduce.go` respectively. The comments in those files should point you in the right direction.

To help you determine if you have correctly implemented doMap() and doReduce(), we have provided you with a Go test suite that checks the correctness of your implementation. These tests are implemented in the file `test_test.go`. To run the tests for the sequential implementation that you have now fixed, run:

```shell
$ cd 6.824
$ export "GOPATH=$PWD"  # go needs $GOPATH to be set to the project's working directory
$ cd "$GOPATH/src/mapreduce"
$ go test -run Sequential
ok  mapreduce	2.694s
```

> You receive full credit for this part if your software passes the Sequential tests (as run by the command above) when we run your software on our machines.

If the output did not show ok next to the tests, your implementation has a bug in it. To give more verbose output, set `debugEnabled = true` in `common.go`, and add `-v` to the test command above. You will get much more output along the lines of:

```shell
$ env "GOPATH=$PWD/../../" go test -v -run Sequential
=== RUN   TestSequentialSingle
master: Starting Map/Reduce task test
Merge: read mrtmp.test-res-0
master: Map/Reduce task completed
--- PASS: TestSequentialSingle (1.34s)
=== RUN   TestSequentialMany
master: Starting Map/Reduce task test
Merge: read mrtmp.test-res-0
Merge: read mrtmp.test-res-1
Merge: read mrtmp.test-res-2
master: Map/Reduce task completed
--- PASS: TestSequentialMany (1.33s)
PASS
ok  	mapreduce	2.672s
```

### Part II: Single-worker word count

Now you will implement word count — a simple Map/Reduce example. Look in main/wc.go; you'll find empty mapF() and reduceF() functions. Your job is to insert code so that wc.go reports the number of occurrences of each word in its input. A word is any contiguous sequence of letters, as determined by unicode.IsLetter.

There are some input files with pathnames of the form `pg-*.txt` in `~/6.824/src/main`, downloaded from Project Gutenberg. Here's how to run wc with the input files:

```shell
$ cd 6.824
$ export "GOPATH=$PWD"
$ cd "$GOPATH/src/main"
$ go run wc.go master sequential pg-*.txt
# command-line-arguments
./wc.go:14: missing return at end of function
./wc.go:21: missing return at end of function
```