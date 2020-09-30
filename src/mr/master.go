package mr

import (
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// the flag of isDebug
const (
	isDebug = false
)

// the type of Task
const (
	Map   = 1
	Reduce = 0
)

// the status of Task
// TaskReady should be deleted
const (
	// 已创建task
	TaskReady    = 0
	// task已进入通道，等待work请求后分配
	TaskQueue = 1
	// task在work上运行
	TaskProcess  = 2
	// task运行失败
	TaskFailed = 3
	// task运行完成
	TaskFinished = 4
)

// the value of some time
const (
	TaskMaxRunTime = time.Second * 10
	TaskScheduleWaitTime = time.Millisecond * 500
)

//
// Task
// store in Master's TaskQueue
// Master receives Workers' request and sent it to Worker
// Worker need it to run Task.
//
type Task struct {
	// the type of Task
	Type int
	// the id of Task
	TaskId int
	// the working file of Task
	File string
	// the nReduce
	NReduce int
}

//
// TaskHead
// the status of Task
// StartTime record before Task is sent to Worker
//
type TaskHead struct {
	// the id of Task
	TaskId int
	// the status of Task
	Status int
	// the worker id of Task
	workerId int
	// start time of Task
	StartTime time.Time
}


//
// Master
//
type Master struct {
	// lock
	mutex sync.Mutex
	// the phase of whole mapreduce job (Map,Reduce)
	phase int
	// the count of registered Workers
	workerCount int
	// the list of file
	// len(files) = nMaps
	files []string
	// the num of Reduce Task
	nReduce int
	// the list of TaskHead
	taskHeadList []TaskHead
	// the queue of Task
	taskQueue chan Task
	// the flag of done
	done bool
}

// initial Master
func (m *Master) initial(files []string, nReduce int){
	m.mutex = sync.Mutex{}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//m.phase = Map
	m.files = files
	m.nReduce = nReduce
	// taskHeadList will be initialed in initMapTasks
	taskQueueSize := math.Max(float64(len(files)),float64(nReduce))
	//
	m.taskQueue = make(chan Task,int(taskQueueSize))
	m.done = false
}

// initial Map/Reduce task
func (m *Master) initTask(Type int, TaskId int) Task{
	if Type == Map{
		return Task{Type: Map, TaskId: TaskId, File: m.files[TaskId],NReduce: m.nReduce}
	}else{
		return Task{Type: Reduce,TaskId: TaskId,File: "",NReduce: m.nReduce}
	}
}

// initial the Map Task and Map Task's TaskHead
// the length of TaskHead = nMaps
func (m *Master) initMapTasks(){
	//
	// maybe don't need lock
	//
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// set the phase of Master: Map
	m.phase = Map
	// TaskHeadList == nil
	// initial Map TaskHead, Map Task in taskQueue
	nMaps := len(m.files)
	m.taskHeadList = make([]TaskHead,nMaps)
	for i:=0;i<nMaps;i++ {
		Dprintf(isDebug,"Master: create TaskHead of Map Task:%d",i)
		m.taskHeadList[i].TaskId = i
		m.taskHeadList[i].Status = TaskQueue
		m.taskQueue <- m.initTask(m.phase,i)
		Dprintf(isDebug,"Master: send Map Task:%d to TaskQueue",i)
	}
	//for index,_ := range m.files{
	//	Dprintf(isDebug,"Master: create TaskHead of Task:%d",index)
	//	// because task will be sent to taskQueue, the status is TaskQueue
	//	m.taskHeadList = append(m.taskHeadList, TaskHead{TaskId: index,Status: TaskQueue})
	//	Dprintf(isDebug,"Master: send Task:%d to TaskQueue",index)
	//	m.taskQueue <- m.initTask(m.phase,index)
	//}
	Dprintf(isDebug,"Master: nMaps = %d",len(m.taskHeadList))
}

// initial the Reduce Task and Reduce Task's TaskHead
// the length of TaskHead = nReduces
func (m *Master) initReduceTasks(){
	//m.mutex.Lock()
	//defer m.mutex.Unlock()
	// set the phase of Master: Reduce
	m.phase = Reduce
	// reset the taskHeadList
	// initial Reduce TaskHead, Reduce Task in taskQueue
	m.taskHeadList = make([]TaskHead,m.nReduce)
	for i:=0;i<m.nReduce;i++ {
		Dprintf(isDebug,"Master: create TaskHead of Reduce Task:%d",i)
		m.taskHeadList[i].TaskId = i
		m.taskHeadList[i].Status = TaskQueue
		m.taskQueue <- m.initTask(m.phase,i)
		Dprintf(isDebug,"Master: send Reduce Task:%d to TaskQueue",i)
	}
}



// schedule Tasks using TaskHeadList
func (m *Master) scheduleTask() {
	//m.mutex.Lock()
	//defer m.mutex.Unlock()
	for !m.done{
		var isAllTasksDone bool = true
		for taskId, taskHead := range m.taskHeadList{
			switch taskHead.Status{
			//case TaskReady:
			//	isAllTasksDone = false
			//	//log.Print("put one task to queue")
			//	// 出现问题,锁mutex已经被占用
			//	m.taskQueue <- m.initTask(m.phase,taskId)
			//	// 状态没有更新成功
			//	m.mutex.Lock()
			//	m.taskHeadList[taskId].Status = TaskQueue
			//	//taskHead.Status = TaskQueue
			//	m.mutex.Unlock()
			case TaskQueue:
				isAllTasksDone = false
			case TaskProcess:
				isAllTasksDone = false
				runTime := time.Now().Sub(taskHead.StartTime)
				// 该task已超时，重新进行分配
				if runTime > TaskMaxRunTime{
					Dprintf(isDebug,"Master: Task:%d by Worker:%d: runs overtime",taskId,taskHead.workerId)
					m.taskQueue <- m.initTask(m.phase,taskId)
					Dprintf(isDebug,"Master: Resend Task:%d to TaskQueue",taskId)
					m.mutex.Lock()
					m.taskHeadList[taskId].Status = TaskQueue
					m.mutex.Unlock()
				}
			case TaskFailed:
				isAllTasksDone = false
				Dprintf(isDebug,"Master: Task:%d by Worker:%d failed",taskId, taskHead.workerId)
				m.taskQueue <- m.initTask(m.phase,taskId)
				Dprintf(isDebug,"Master: Resend Task:%d to TaskQueue",taskId)
				m.mutex.Lock()
				m.taskHeadList[taskId].Status = TaskQueue
				m.mutex.Unlock()
			//case TaskFinished:
				// do nothing
			}
		}
		// all map tasks finished
		if isAllTasksDone && m.phase == Map{
			Dprintf(isDebug,"-------------finished Map phase-------------")
			// initial Reduce Task and TaskHead
			m.initReduceTasks()
			// set isAllTasksDone false, otherwise program will run next if{}
			isAllTasksDone = false
		}
		if isAllTasksDone && m.phase == Reduce{
			Dprintf(isDebug,"------------finished Reduce phase-----------")
			m.mutex.Lock()
			// the whole mapreduce job is done
			// program will exit if done == true
			m.done = true
			m.mutex.Unlock()
		}
	}
	// wait some time to next schedule
	time.Sleep(TaskScheduleWaitTime)
}

// Your code here -- RPC handlers for the worker to call.
// handle Worker's register
func (m *Master) AcceptRegister(args *RegisterArgs, reply *RegisterReply) error {
	// worker id [0,...]
	reply.Id = m.workerCount
	// send nMaps to Worker
	reply.NMaps = len(m.files)
	m.mutex.Lock()
	m.workerCount++
	defer m.mutex.Unlock()
	return nil
}

// handle Worker's request for task
func (m *Master) SendTask(args *TaskArgs,reply *TaskReply) error{
	//m.mutex.Lock()
	//defer m.mutex.Unlock()
	// if taskQueue is nil, Worker will block here
	task := <- m.taskQueue
	// Task will be sent to task, update it's TaskHead
	m.mutex.Lock()
	m.taskHeadList[task.TaskId].Status = TaskProcess
	m.taskHeadList[task.TaskId].StartTime = time.Now()
	m.taskHeadList[task.TaskId].workerId = args.WorkId
	m.mutex.Unlock()
	Dprintf(isDebug,"Master: Send Task:%v to Worker:%v",task.TaskId,args.WorkId)
	reply.Task = task
	return nil
}

// handle Worker's Task Report
func (m *Master) ReceiveReport(args *ReportArgs,reply *ReportReply) error{
	TaskId := args.TaskId
	taskHead := m.taskHeadList[TaskId]
	// prevent case: Task type Map but Master Phase Reduce
	if args.Type == m.phase && args.WorkId == taskHead.workerId {
		// Task failed
		if !args.Result{
			m.taskHeadList[TaskId].Status = TaskFailed
			return nil
		}
		// Task finished
		m.mutex.Lock()
		m.taskHeadList[TaskId].Status = TaskFinished
		m.mutex.Unlock()
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	if m.done {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// initial the master
	m.initial(files,nReduce)
	// initial the task head list of map phase
	m.initMapTasks()
	// schedule tasks
	go m.scheduleTask()

	m.server()
	return &m
}
