package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker definitions
type OneWorker struct {
	id      int
	nMaps   int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// register Worker to Master
func (worker *OneWorker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	// use RPC to register
	if ok := call("Master.AcceptRegister", &args, &reply); !ok {
		log.Fatal("Worker failed to register itself to master.")
	}
	worker.id = reply.Id
	worker.nMaps = reply.NMaps
}

// request a task from master
func (worker *OneWorker) requestTask(WorkId int) Task{
	args := TaskArgs{WorkId: WorkId}
	reply := TaskReply{}
	if ok := call("Master.SendTask",&args,&reply); !ok{
		log.Fatal("Worker failed to request a task.")
	}
	log.Printf("---work:%d request task: %d",worker.id,reply.Task.TaskId)

	return reply.Task
}

func (worker *OneWorker) reportTask(TaskId int,Type int,WorkerId int,Result bool){
	args := ReportArgs{TaskId: TaskId,Type: Type,WorkId: WorkerId,Result: Result}
	reply := ReportReply{}
	if ok := call("Master.ReceiveReport",&args,&reply); !ok{
		log.Fatal("Worker failed to report.")
	}

}


// do Map task
func (worker *OneWorker) doMap(t Task) {
	intermediate := []KeyValue{}
	fileName := t.File
	nReduce := t.NReduce
	taskId := t.TaskId
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		worker.reportTask(taskId,t.Type,worker.id,false)
		log.Fatalf("Worker can't open the file: %s.", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		worker.reportTask(taskId,t.Type,worker.id,false)
		log.Fatalf("Worker can't read the file: %s.", fileName)
	}
	kva := worker.mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	hashedKv := make([][]KeyValue,nReduce)
	for _,kv := range intermediate{
		key := kv.Key
		index := ihash(key)%nReduce
		hashedKv[index] = append(hashedKv[index],kv)
	}
	// write intermediate to file
	for i,kvs := range hashedKv{
		outFileName := fmt.Sprintf("mr-%d-%d",taskId,i)

		if Exists(outFileName){
			os.Remove(outFileName)
		}

		outFile,err := os.Create(outFileName)
		if err!=nil{
			worker.reportTask(taskId,t.Type,worker.id,false)
			log.Fatal("Worker failed to create file: %s",outFileName)
		}
		enc := json.NewEncoder(outFile)
		for _,kv := range kvs{
			if err:= enc.Encode(&kv);err!=nil{
				worker.reportTask(taskId,t.Type,worker.id,false)
				log.Fatal("Worker failed to encode kv.")
				// 应向master报告task失败
			}
		}
		if err := outFile.Close();err != nil{
			worker.reportTask(taskId,t.Type,worker.id,false)
			log.Fatal("Worker failed to close the new file: %s.",outFileName)
		}
	}
	// task finished
	worker.reportTask(taskId,t.Type,worker.id,true)

}

func (worker *OneWorker) getInterFileName(mapTaskId,reduceTaskId int) string{
	return fmt.Sprintf("mr-%d-%d",mapTaskId,reduceTaskId)
}

func Exists(path string) bool {
	_, err := os.Stat(path)    //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}


type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// do Reduce task
func (worker *OneWorker) doReduce(t Task){
	//maps := make(map[string][]string)
	intermediate := []KeyValue{}
	// 读取中间KV值
	for i:=0;i<worker.nMaps;i++{
		interFileName := worker.getInterFileName(i,t.TaskId)
		//log.Printf(interFileName)
		interFile,err := os.Open(interFileName)
		if err !=nil{
			worker.reportTask(t.TaskId,t.Type,worker.id,false)
			log.Fatal("Worker failed to open the intermediate file: %s.",interFileName)
		}
		dec := json.NewDecoder(interFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
			//if _,ok:=maps[kv.Key];!ok{
			//	maps[kv.Key] = append(maps[kv.Key],kv.Value)
			//}
		}
	}

	//res := make([]string, 0, 100)
	//for k, v := range maps {
	//	res = append(res, fmt.Sprintf("%v %v\n", k, worker.reducef(k, v)))
	//}
	//
	//if err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d",t.TaskId), []byte(strings.Join(res, "")), 0600); err != nil {
	//	worker.reportTask(t.TaskId,t.Type,worker.id,false)
	//}
	//
	//worker.reportTask(t.TaskId,t.Type,worker.id,true)

	sort.Sort(ByKey(intermediate))
	outFileName := fmt.Sprintf("mr-out-%d",t.TaskId)
	if Exists(outFileName){
		os.Remove(outFileName)
	}
	ofile,err := os.Create(outFileName)

	if err!=nil{
		worker.reportTask(t.TaskId,t.Type,worker.id,false)
		log.Fatal("Worker failed to create the final file: %s.",outFileName)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := worker.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		if _,err := fmt.Fprintf(ofile, "%v %vaa\n", intermediate[i].Key, output);err!=nil{
			worker.reportTask(t.TaskId,t.Type,worker.id,false)
		}

		i = j
	}
	if err:=ofile.Close();err!=nil{
		worker.reportTask(t.TaskId,t.Type,worker.id,false)
	}
	worker.reportTask(t.TaskId,t.Type,worker.id,true)
	log.Printf("worker:%d successfully hhhhh finish reduce task:%d",worker.id,t.TaskId)


}

// Worker不停地申请并运行task，当master完成后，所有worker被强制退出
func (worker *OneWorker) doTask(){
	for{
		task := worker.requestTask(worker.id)
		if task.Type == Map{
			log.Printf("%d",worker.nMaps)
			worker.doMap(task)
			log.Printf("worker:%d hahahah finish one map task:%d",worker.id,task.TaskId)
		}else{
			log.Printf("worker:%d is doing reduce task.",worker.id)
			worker.doReduce(task)
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	worker := OneWorker{mapf: mapf,reducef:reducef}
	// initial the worker id and worker nMaps
	worker.register()
	// worker申请task并执行
	worker.doTask()

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
