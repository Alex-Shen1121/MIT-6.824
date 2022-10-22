package mr

import (
	"fmt"
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
	Wait      = 2
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
	// 操作队列的时候要加锁
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 重启挂了的任务（超过10s）
	curTime := time.Now().Unix()
	for i, task := range c.mapTasksInProgress {
		if curTime-task.TimeStamp > 10 {
			c.mapTasksReady[i] = task
			delete(c.mapTasksInProgress, i)
			fmt.Printf("重启Map任务 %d", i)

		}
	}
	for i, task := range c.reduceTasksInProgress {
		if curTime-task.TimeStamp > 10 {
			c.reduceTasksReady[i] = task
			delete(c.reduceTasksInProgress, i)
			fmt.Printf("重启Reduce任务 %d", i)

		}
	}

	// 处于map阶段
	if len(c.mapTasksReady) > 0 {
		// 因为map遍历是随机的，因此随机取一个任务
		for i, task := range c.mapTasksReady {
			// 设置当前task的时间戳
			task.TimeStamp = time.Now().Unix()
			// 将这个任务发给reply
			reply.TaskInfo = task
			// 将这个任务转移到inProgress队列
			c.mapTasksInProgress[i] = task
			delete(c.mapTasksReady, i)
			fmt.Printf("发送Map任务给Worker", reply.TaskInfo)
			return nil
		}
	} else if len(c.mapTasksInProgress) > 0 {
		// 仍然有map任务在执行，则等待
		reply.TaskInfo = Task{TaskType: Wait}
		return nil
	}

	// 检测是否完成map阶段
	// 在进入reduce阶段前，要生成所有的Reduce Task
	if !c.reduceReady {
		for i := 0; i < c.NReduce; i++ {
			c.reduceTasksReady[i] = Task{
				TaskType:  Reduce,
				TaskID:    i,
				NReduce:   c.NReduce,
				MMap:      c.MMap,
				TimeStamp: time.Now().Unix()}
		}
		c.reduceReady = true
	}

	// 处于reduce阶段
	if len(c.reduceTasksReady) > 0 {
		// 因为map遍历是随机的，因此随机取一个任务
		for i, task := range c.reduceTasksReady {
			// 设置当前task的时间戳
			task.TimeStamp = time.Now().Unix()
			// 将这个任务发给reply
			reply.TaskInfo = task
			// 将这个任务转移到inProgress队列
			c.reduceTasksInProgress[i] = task
			delete(c.reduceTasksReady, i)
			fmt.Printf("发送Reduce任务给Worker", reply.TaskInfo)
			return nil
		}
	} else if len(c.reduceTasksInProgress) > 0 {
		// 仍然有reduce任务在执行，则等待
		reply.TaskInfo = Task{TaskType: Wait}
		return nil
	} else {
		// 完成了所有任务
		reply.TaskInfo = Task{TaskType: Completed}
	}

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
