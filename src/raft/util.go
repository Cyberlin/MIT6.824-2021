package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Debugging
const debug = false

type logTopic string

var debugStart time.Time
var debugVerbosity int

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}
func getInvokerLocation(skipNumber int) string {
	_, file, line, ok := runtime.Caller(skipNumber)
	if !ok {
		return ""
	}
	simpleFileName := ""
	if index := strings.LastIndex(file, "/"); index > 0 {
		simpleFileName = file[index+1 : len(file)]
	}

	return fmt.Sprintf("(%s:%d):", simpleFileName, line)
}

func (this *Raft) Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		tag := fmt.Sprintf("[%s]", string(topic))
		idMark := " "
		if this.me == this.leaderId {
			idMark = "*"
		}
		invorkPlace := getInvokerLocation(2)
		prefix := fmt.Sprintf("%06d %v S%d%s %s", time, tag, this.me, idMark, invorkPlace)
		format = prefix + format
		log.Printf(format, a...)
	}
}
func Dprintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		tag := fmt.Sprintf("[%s]", string(topic))

		invorkPlace := getInvokerLocation(2)
		prefix := fmt.Sprintf("%06d %v %s", time, tag, invorkPlace)
		format = prefix + format
		log.Printf(format, a...)
	}
}
func Assert(val interface{}, expect interface{}, note ...interface{}) {
	if debugVerbosity >= 1 {
		if val != expect {
			timeA := time.Since(debugStart).Microseconds()
			timeA /= 100
			tag := "[ASSERT]"

			invorkPlace := getInvokerLocation(2)
			info := fmt.Sprintf("%06d %v %s: %s", timeA, tag, invorkPlace, note)

			log.Panicln(info)
		}
	}
}
func Min(nums ...int) int {
	Assert(len(nums) == 0, false, "Bad min usage!")
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	return nums[0]
}
