package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Worker's register RPC
type RegisterArgs struct {
}

type RegisterReply struct {
	Id int
	NMaps int

}

// Worker's request of task
type TaskArgs struct {
	WorkId int
}

type TaskReply struct {
	Task Task
}

type ReportArgs struct{
	TaskId int
	Type int
	WorkId int
	Result bool
}

type ReportReply struct{

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// Output log when debug
func Dprintf(isDebug bool,format string,v ...interface{}){
	if isDebug {
		log.Printf(format,v)
	}
}
