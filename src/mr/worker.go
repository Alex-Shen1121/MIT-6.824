package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//// Your worker implementation here.
	// 不断向coordinator更新状态
	for {
		args := RPCArgs{}
		reply := RPCReply{}

		// 向coordinator索取任务
		ok := call("Coordinator.GiveTask", &args, &reply)

		if ok {
			//fmt.Printf("reply.TaskInfo : %v\n", reply.TaskInfo)
			switch reply.TaskInfo.TaskType {
			// 执行Map任务
			case Map:
				fmt.Printf("执行 Map 任务")
				doMap(&reply.TaskInfo, mapf)
			// 执行Reduce任务
			case Reduce:
				fmt.Printf("执行 Reduce 任务")
				doReduce(&reply.TaskInfo, reducef)
			// 闲置
			case Idle:
				fmt.Printf("当前 Worker 空闲，等待一秒")
				time.Sleep(time.Second)
				continue
			case Completed:
				fmt.Printf("完成所有 Task 任务")
				break
			}
			// 告知coordinator任务完成
			args.TaskInfo = reply.TaskInfo
			call("Coordinator.TaskDone", &args, &reply)
		} else {
			fmt.Printf("RPC 调用失败")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// Map方法
func doMap(task *Task, mapf func(string, string) []KeyValue) {
	fmt.Printf("Map worker get task %d-%s\n", task.TaskID, task.Filename)

	// 创建中间临时文件
	// intermediate[i][] 说明 传入第i个 reduce task
	// 第二维表示中间结果
	intermediate := make([][]KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 执行map方法并写入中间文件
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%task.NReduce] =
			append(intermediate[ihash(kv.Key)%task.NReduce], kv)
	}

	// 将intermediate写入临时文件夹
	// 命名规则参考 hint 第6条
	for i := 0; i < task.NReduce; i++ {
		// 改分区没有value要写
		if len(intermediate[i]) == 0 {
			continue
		}
		outputFileName := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		//outputFile, _ := ioutil.TempFile("./tmp/", "tmp_")
		outputFile, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			fmt.Printf("文件创建失败")
		}
		enc := json.NewEncoder(outputFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Json encode error: Key-%s, Value-%s", kv.Key, kv.Value)
			}
		}
		outputFile.Close()
		os.Rename("./tmp/"+outputFile.Name(), outputFileName)
	}
}

// Reduce方法
func doReduce(task *Task, reducef func(string, string) []KeyValue) {
	fmt.Printf("Reduce worker get task %d\n", task.TaskID)

}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
