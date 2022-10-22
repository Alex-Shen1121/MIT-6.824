package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex

	mapTasksReady      map[int]Task
	mapTasksInProgress map[int]Task

	reduceTasksReady      map[int]Task
	reduceTasksInProgress map[int]Task

	NReduce int
	MMap    int

	reduceReady bool
}

// Task info
type Task struct {
	Filename  string
	TaskType  int
	TaskID    int
	TimeStamp int64 // in seconds
	NReduce   int   // reduce 任务数量
	MMap      int   // map 任务数量
}

// Task type
const (
	Map       = 0
	Reduce    = 1
	Idle      = 2
	Completed = 3
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
	return nil
}

// GiveTask : 返回reply，告知worker任务信息
func (c *Coordinator) GiveTask(args *RPCArgs, reply *RPCReply) error {
	c.
	return nil
}

// 根据args判断任务完成情况
func (c *Coordinator) TaskDone(args *RPCArgs, reply *RPCReply) error {
	reply.TaskInfo = args.TaskInfo
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化队列
	c.mapTasksReady = make(map[int]Task)
	c.reduceTasksReady = make(map[int]Task)
	c.mapTasksInProgress = make(map[int]Task)
	c.reduceTasksInProgress = make(map[int]Task)

	// 初始化
	numFile := len(files)
	for i, file := range files {
		c.mapTasksReady[i] = Task{
			Filename:  file,
			TaskType:  Map,
			TaskID:    i,
			NReduce:   nReduce,
			MMap:      numFile,
			TimeStamp: time.Now().Unix()}
	}
	//c.reduceReady = false // 这个字段还没提到，待会儿说
	c.NReduce = nReduce
	c.MMap = numFile

	c.server()
	return &c
}
