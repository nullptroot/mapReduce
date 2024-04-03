package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
// map函数的返回值
type KeyValue struct {
	Key   string
	Value string
}

// keyValue的切片，方便后续的排序
type ByKey []KeyValue

// 实现下面的方法，就可以使用sort函数了，sort函数接受一个实现下面三个函数的接口
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
// hash函数，主要是map任务处理完文件后，需要hash成reduce数量的文件，根据key进行hash
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// worker的主函数  一个进程调用这个函数，死循环的处理事件
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		//先向master获取任务
		task := getTask()
		// 根据任务的类型来做不同的处理
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

func reducer(task *Task, reducef func(string, []string) string) {
	// 首先先读取中间文件
	intermediates := *readFromLocalFile(task.Intermediates)
	// 根据key进行排序
	sort.Sort(ByKey(intermediates))
	//  获取当前路径
	// dir, _ := os.Getwd()
	curDir, _ := os.Getwd()
	os.Chdir(curDir + "/output/")
	defer os.Chdir(curDir)
	dir, _ := os.Getwd()
	// 创建临时文件
	tmpFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	//  下面是统计词频的操作了
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		values := []string{}
		// 这里就是把相同的单词append到一起，也就是values == "1111111111"长度就是词频
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}

		// 获取到了具体的词频
		output := reducef(intermediates[i].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}

	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tmpFile.Name(), oname)
	TaskCompleted(task)

}

func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Failed to open file"+filePath, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// map任务的处理
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	//首先读取任务的输入文件
	content, err := os.ReadFile(task.Input)

	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	// 调用用户的mapf来对文件内容进行处理
	intermediates := mapf(task.Input, string(content))

	// 创建一个缓冲区 来将处理后的数据分割成多个中间文件
	buffer := make([][]KeyValue, task.NReduce)
	// 根据key对处理的文件进行hash到不同的桶
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReduce
		buffer[slot] = append(buffer[slot], intermediate)
	}
	// 存储生成中间文件的文件名
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		// writeToLocalFile就是生成文件，并将内容写到文件中
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	// 将中间文件信息赋值给任务
	task.Intermediates = mapOutput
	// 调用任务完成函数 此函数会rpc调用Coordinator.TaskCompleted方法
	TaskCompleted(task)

}

// 调用任务完成函数 此函数会rpc调用Coordinator.TaskCompleted方法
func TaskCompleted(task *Task) {
	reply := ExampleArgs{}
	call("Coordinator.TaskCompleted", task, &reply)
}

// 把kv写到中间文件中，此时并没有顺序
func writeToLocalFile(x, y int, kvs *[]KeyValue) string {
	curDir, _ := os.Getwd()
	os.Chdir(curDir + "/tmp/")
	defer os.Chdir(curDir)
	dir, _ := os.Getwd()
	//创建临时文件 *表示随机字符，避免冲突
	tmpFile, err := os.CreateTemp(dir, "mr-tmp-*")

	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// 生成一个json的编码器，可以直接把编码后的内容写到tmpFile中去
	enc := json.NewEncoder(tmpFile)
	// 写文件
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tmpFile.Close()
	// 重命名
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tmpFile.Name(), outputName)
	// 返回中间文件信息
	return filepath.Join(dir, outputName)
}

// 通过调用rpc方法Coordinator.AssignTask获取到任务
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	// rpc成功返回reply
	if call("Coordinator.AssignTask", &args, &reply) {
		return reply
	}
	// 否则直接返回Exit状态的任务即可
	return Task{TaskState: Exit}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// 调用rpc的例子
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// 调用的具体实现
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// 获取socket通信的本地文件名
	sockname := coordinatorSock()
	// 建立http连接
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// reply = nil
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()
	// 发送调用请求
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
