package shardkv

import (
	"fmt"
	"os"
	"shardmaster"
	"strconv"
)

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

// predict value that would result from an Append
func NextValue(prev string, val string) string {
	return prev + val
}

func mcleanup(sma []*shardmaster.ShardMaster) {
	for i := 0; i < len(sma); i++ {
		if sma[i] != nil {
			sma[i].Kill()
		}
	}
}

func TestConcurretnUnreliable(t *tetsing.T) {
	fmt.Print("Test: Concurrent Put/Get/Move (unreliable) ...\n")
	doConcurrent(t, true)
	fmt.Println("Concurrent Feature completed!")
}
