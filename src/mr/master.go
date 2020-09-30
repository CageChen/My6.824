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

// the type of task
const (
	Map   = 1
	Reduce = 0
)

// the stat of task
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
	ScheduleWaitTime = time.Millisecond * 500
)

//
// task类
// 用于发送给worker
// 包含 task类型，task工作文件，task的id，
//

type Task struct {
	// the type of task
	Type int
	// the id of task
	TaskId int
	// the working file of task
	File string
	// the nReduce
	NReduce int
}

//
// master自身记录当前所处阶段，并在每个task中给出具体阶段
// master首先初始化TaskStat，并根据其状态决定是否新创建task
// 当map阶段已全部完成时，清空TaskStat，并创建Reduce阶段的TaskStat
// map阶段和reduce阶段的创建步骤不同，因为两个阶段的任务数并不相同
// 当reduce阶段后，完成Done()
//

//
// TaskHead
// 用于记录task的状态
// 保存在master中
// TaskHead workId在被分配后进行更新
// StartTime 在task被分配后记录
//
type TaskHead struct {
	// the id of task
	TaskId int
	// the stat of task
	Status int
	// the worker id of task
	workerId int
	// start time of task
	StartTime time.Time
}



type Master struct {
	// Your definitions here.
	// lock
	mutex sync.Mutex
	// the phase of whole mr
	phase int
	// count of registered workers
	workerCount int
	// the list of file
	// len(files) = nMap
	files []string
	// the num of Reduce tasks
	nReduce int
	// the list of TaskHead
	taskHeadList []TaskHead
	// the queue of task
	taskQueue chan Task
	// the channel of done
	done bool

}

// 对master进行初始化
func (m *Master) initial(files []string, nReduce int){
	m.mutex = sync.Mutex{}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//m.phase = Map
	m.files = files
	m.nReduce = nReduce
	// taskHeadList在之后进行初始化
	taskQueueSize := math.Max(float64(len(files)),float64(nReduce))
	// 被阻塞？？？
	m.taskQueue = make(chan Task,int(taskQueueSize))
	m.done = false
}

// initMapTasks 仅初始化TaskHead
// TaskHead 长度为nMaps
func (m *Master) initMapTasks(){
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// 将master的阶段状态设为Map
	m.phase = Map
	// 当前TaskHeadList为空
	for index,file := range m.files{
		log.Printf("create task head for %s",file)
		m.taskHeadList = append(m.taskHeadList, TaskHead{TaskId: index,Status: TaskQueue})
		m.taskQueue <- m.initTask(m.phase,index)
		log.Printf("current the length of task head: %d",len(m.taskHeadList))
		// Map Task在之后的调度环节创建
	}
	log.Printf("the length of task head: %d",len(m.taskHeadList))
}

// initReduceTasks
func (m *Master) initReduceTasks(){
	//m.mutex.Lock()
	//defer m.mutex.Unlock()
	m.phase = Reduce
	// 设为空
	m.taskHeadList = make([]TaskHead,m.nReduce)
	for i:=0;i<m.nReduce;i++ {
		m.taskHeadList[i].TaskId = i
		m.taskHeadList[i].Status = TaskQueue
		m.taskQueue <- m.initTask(Reduce,i)
	}
	//for i:=0;i<m.nReduce;i++ {
	//	m.taskHeadList = append(m.taskHeadList,TaskHead{TaskId: i,Status: TaskReady})
		// Reduce Task在之后创建
	//}
}

// inital one map or reduce task
func (m *Master) initTask(Type int, TaskId int) Task{
	if Type == Map{
		//log.Print("finish initial one map task")
		return Task{Type: Map, TaskId: TaskId, File: m.files[TaskId],NReduce: m.nReduce}
	}else{
		return Task{Type: Reduce,TaskId: TaskId,File: "",NReduce: m.nReduce}
	}
}

// handle worker's request of task
func (m *Master) scheduleTask() {
	//m.mutex.Lock()
	//defer m.mutex.Unlock()
	for !m.done{
		var isAllDone bool = true
		//for taskid, taskHead := range m.taskHeadList{
		//	log.Printf("task id: %d, taskStatus: %d",taskid,taskHead.Status)
		//}
		//taskHeadList中不一定是占满的
		for taskId, taskHead := range m.taskHeadList{
			//log.Printf("schedule task:%d",taskId)
			switch taskHead.Status{
			case TaskReady:
				//log.Print("ready one task")
				isAllDone = false
				//log.Print("put one task to queue")
				// 出现问题,锁mutex已经被占用
				m.taskQueue <- m.initTask(m.phase,taskId)
				// 状态没有更新成功
				m.mutex.Lock()
				m.taskHeadList[taskId].Status = TaskQueue
				//taskHead.Status = TaskQueue
				m.mutex.Unlock()

				//log.Print(TaskQueue)
				//log.Print("already put task to queue")
			case TaskQueue:
				isAllDone = false
			case TaskProcess:
				isAllDone = false
				runTime := time.Now().Sub(taskHead.StartTime)
				// 该task已超时，重新进行分配
				if runTime > TaskMaxRunTime{
					m.taskQueue <- m.initTask(m.phase,taskId)

					m.mutex.Lock()
					m.taskHeadList[taskId].Status = TaskQueue
					m.mutex.Unlock()
				}
			case TaskFailed:
				log.Printf("task:%d failed",taskId)
				isAllDone = false
				m.taskQueue <- m.initTask(m.phase,taskId)
				m.mutex.Lock()
				m.taskHeadList[taskId].Status = TaskQueue
				m.mutex.Unlock()
			//case TaskFinished:
				// do nothing
			}
			//log.Print("finished one schedule task")
		}
		// all tasks of map are finished
		if isAllDone && m.phase == Map{
			log.Print("-------------finished map phase")
			// 对Reduce的taskHead进行初始化
			log.Print("before init reduce task")
			m.initReduceTasks()
			log.Print("after init reduce task")
			// 将isAllDone设为false，否则将直接进入下个if导致master结束
			isAllDone = false
		}
		if isAllDone && m.phase == Reduce{
			// 标记mr任务已完成，等待mrmaster结束任务
			log.Print("-------------finished reduce phase")
			m.mutex.Lock()
			m.done = true
			m.mutex.Unlock()
		}
	}
	//完成一次调度后，睡眠一段时间
	time.Sleep(ScheduleWaitTime)
}



// Your code here -- RPC handlers for the worker to call.

// handle worker's register
func (m *Master) AcceptRegister(args *RegisterArgs, reply *RegisterReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// worker id [0,...]
	reply.Id = m.workerCount
	// nMaps
	reply.NMaps = len(m.files)
	m.workerCount++
	return nil
}

func (m *Master) SendTask(args *TaskArgs,reply *TaskReply) error{
	//m.mutex.Lock()
	//defer m.mutex.Unlock()
	// 若taskQueue为空，则会在此阻塞
	task := <- m.taskQueue
	// task已被分配，更新其状态和运行时间
	taskHead := m.taskHeadList[task.TaskId]
	m.mutex.Lock()
	m.taskHeadList[task.TaskId].Status = TaskProcess
	m.taskHeadList[task.TaskId].StartTime = time.Now()
	m.taskHeadList[task.TaskId].workerId = args.WorkId
	m.mutex.Unlock()

	log.Printf("-------------已分配task:%d,workId:%d",taskHead.TaskId,taskHead.workerId)
	// 可以在这里对WorkId和WorkerCount进行比较，避免有其他Worker加入
	reply.Task = task
	return nil
}

func (m *Master) ReceiveReport(args *ReportArgs,reply *ReportReply) error{
	TaskId := args.TaskId
	taskHead := m.taskHeadList[TaskId]

	if args.Type == m.phase && args.WorkId == taskHead.workerId {
		// task failed
		if !args.Result{
			m.taskHeadList[TaskId].Status = TaskFailed
			return nil
		}
		// task finished
		// 只有当已完成的task的type和master当前阶段一样，才可对task head进行更新
		// 否则会出现奇怪的错误
		m.mutex.Lock()

		m.taskHeadList[TaskId].Status = TaskFinished
		m.mutex.Unlock()
	}//else{
		//可以加一个报错
	//}
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
	if m.done{
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
