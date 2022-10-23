package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
				fmt.Println("执行 Map 任务")
				doMap(&reply.TaskInfo, mapf)
			// 执行Reduce任务
			case Reduce:
				fmt.Println("执行 Reduce 任务")
				doReduce(&reply.TaskInfo, reducef)
			// 闲置
			case Wait:
				fmt.Println("当前 Worker 空闲，等待一秒")
				time.Sleep(time.Second)
				continue
			case Completed:
				fmt.Println("已完成所有 Task 任务。 Worker %d 退出。", reply.TaskInfo.TaskID)
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
	file, err := os.Open("./" + filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 执行map方法并写入中间文件
	fmt.Println(os.Getwd())
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
		outputFile, _ := ioutil.TempFile("./", "tmp_")
		//outputFile, err := os.OpenFile("./tmp/"+outputFileName, os.O_WRONLY|os.O_CREATE, 0666)
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
		os.Rename(outputFile.Name(), outputFileName)
	}
}

// Reduce方法
func doReduce(task *Task, reducef func(string, []string) string) {
	fmt.Printf("Reduce worker get task %d\n", task.TaskID)

	// 从tmp文件中读取中间文件
	intermediate := make([]KeyValue, 0)
	for i := 0; i < task.MMap; i++ {
		// 读取文件
		inputFileName := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		inputFile, err := os.OpenFile("./"+inputFileName, os.O_RDWR, 0666)
		if err != nil {
			fmt.Printf("打开文件失败")
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	ofile, _ := ioutil.TempFile("./", "tmp_")

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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)
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
