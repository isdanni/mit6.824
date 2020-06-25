# mit6.824 Distributed Systems

Spring 2020. Implemented with Go 1.10.

[https://pdos.csail.mit.edu/6.824/schedule.html](https://pdos.csail.mit.edu/6.824/schedule.html)

### What is 6.824 about?

6.824 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

### Lab

- Lab 1: MapReduce
- Lab 2: replication for fault-tolerance using Raft
- Lab 3: fault-tolerant key/value store
- Lab 4: sharded key/value store

### Set up

1. Install golang, and setup golang environment variables and directories. Click [here](https://github.com/golang/go/wiki/SettingGOPATH) to learn it.

2. Setup the labs.
```shell
cd $GOPATH
git clone https://github.com/isdanni/mit6.824.git
cd mit6.824
export GOPATH=$GOPATH:$(pwd)
```

### Notes

Tne source code also contains `/kvpaxos`, the implementation of consensus algorithm [paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science));
