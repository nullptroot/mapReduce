# mapreduce

MIT 6.824 (2020 Spring) Lab1

Lab code has changed in the new semester. Here is the old version. Don't copy my answer.

---
## 参考大神代码 https://github.com/s09g/mapreduce-go
## MapReduce的执行流程

根据论文第三节，MapReduce的执行流程分这么几步：

> 1. The MapReduce library in the user program first splits the input files into M pieces of typically 16 megabytes to 64 megabytes (MB) per piece (controllable by the user via an optional parameter). It then starts up many copies of the program on a cluster of machines.

启动MapReduce, 将输入文件切分成大小在16-64MB之间的文件。然后在一组多个机器上启动用户程序

> 2. One of the copies of the program is special – the master. The rest are workers that are assigned work by the master. There are M map tasks and R reduce tasks to assign. The master picks idle workers and assigns each one a map task or a reduce task.

其中一个副本将成为master, 余下成为worker. master给worker指定任务（M个map任务，R个reduce任务）。master选择空闲的worker给予map或reduce任务

> 3. A worker who is assigned a map task reads the contents of the corresponding input split. It parses key/value pairs out of the input data and passes each pair to the user-defined Map function. The intermediate key/value pairs produced by the Map function are buffered in memory.

Map worker 接收切分后的input，执行Map函数，将结果缓存到内存

> 4. Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function. The locations of these buffered pairs on the local disk are passed back to the master, who is responsible for forwarding these locations to the reduce workers.

缓存后的中间结果会周期性的写到本地磁盘，并切分成R份（reducer数量）。R个文件的位置会发送给master, master转发给reducer

> 5. When a reduce worker is notified by the master about these locations, it uses remote procedure calls to read the buffered data from the local disks of the map workers. When a reduce worker has read all intermediate data, it sorts it by the intermediate keys so that all occurrences of the same key are grouped together. The sorting is needed because typically many different keys map to the same reduce task. If the amount of intermediate data is too large to fit in memory, an external sort is used.

Reduce worker 收到中间文件的位置信息，通过RPC读取。读取完先根据中间<k, v>排序，然后按照key分组、合并。

> 6. The reduce worker iterates over the sorted intermediate data and for each unique intermediate key encountered, it passes the key and the corresponding set of intermediate values to the user’s Reduce function. The output of the Reduce function is appended to a final output file for this reduce partition.

Reduce worker在排序后的数据上迭代，将中间<k, v> 交给reduce 函数处理。最终结果写给对应的output文件（分片）

> 7. When all map tasks and reduce tasks have been completed, the master wakes up the user program. At this point, the MapReduce call in the user program returns back to the user code.

所有map和reduce任务结束后，master唤醒用户程序

---

## MapReduce的实现

### Master的数据结构设计

这一部分对论文的3.2节有所改动，相比于原论文有所简化。

论文提到每个(Map或者Reduce)Task有分为idle, in-progress, completed 三种状态。

```go
type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)

```

Master存储这些Task的信息。与论文不同的是，这里我并没有保留worked的ID，因此master不会主动向worker发送`心跳检测`

```go
// master中包装任务的结构体
type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus //任务的状态
	StartTime     time.Time             //处理任务的起始时间
	TaskReference *Task                 //具体的任务结构
}
```

此外Master存储Map任务产生的R个中间文件的信息。

```go
type Coordinator struct {
	TaskQueue        chan *Task               // 任务队列  后面是一个有缓冲的chan
	TaskMeta         map[int]*CoordinatorTask //管理所有的任务map，使用任务id映射具体任务
	CoordinatorPhase State                    //master的状态
	NReduce          int                      //多少个reduce
	InputFiles       []string                 //任务的输入文件
	Intermediates    [][]string               //map产生的中间文件
	mu               sync.Mutex               //锁来控制互斥
}
```
Map和Reduce的Task应该负责不同的事情，但是在实现代码的过程中发现同一个Task结构完全可以兼顾两个阶段的任务。

```go
type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}
```

此外我将task和master的状态合并成一个State。task和master的状态应该一致。如果在Reduce阶段收到了迟来MapTask结果，应该直接丢弃。

```go
type State int

// 主master的状态
const (
	Map    State = iota //当前系统处于map阶段
	Reduce              //当前系统处于reduce阶段
	Exit                //当前系统处于exit阶段 也就是所有任务都完成了
	Wait                //当前系统处于wait阶段
)
```

### MapReduce执行过程的实现

**1. 启动master**

```go
// 根据参数创建一个Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 创建一个Coordinator充当master，来协调各个worker
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskMeta:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}
	// 系统刚开始要先创建map任务，然后后续的rpc调用逐渐的改变状态
	c.CreateMapTask()

	// Your code here.

	c.server()
	// 判断超时的worker 重新分配任务
	go c.catchTimeout()

	return &c
}
```

**2. master监听worker RPC调用，分配任务**

```go
// worker申请任务的rpc接口  任务会通过reply发送给worker
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 当TaskQueue大于0时，才能分配任务，然后改变任务的状态
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit { // master退出了 worker也就该退出了
		*reply = Task{TaskState: Exit}
	} else { //其他情况就让worker等会
		*reply = Task{TaskState: Wait}
	}
	return nil
}
```

**3. 启动worker**

```go
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
```

**4. worker向master发送RPC请求任务**

```go
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
```

**5. worker获得MapTask，交给mapper处理**

```go
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
```

**6. worker任务完成后通知master**

```go
// 调用任务完成函数 此函数会rpc调用Coordinator.TaskCompleted方法
func TaskCompleted(task *Task) {
	reply := ExampleArgs{}
	call("Coordinator.TaskCompleted", task, &reply)
}
```

**7. master收到完成后的Task**

+ 如果所有的MapTask都已经完成，创建ReduceTask，转入Reduce阶段
+ 如果所有的ReduceTask都已经完成，转入Exit阶段

```go
// 每当worker完成一个任务 不管时map还是reduce，都会rpc调用TaskCompleted 让Coordinator来处理完成的任务
func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 当worker的状态和master的状态不一致，或者master显示当前worker处理的任务已经完成了
	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	// 处理这个完成的任务
	go c.processTaskResult(task)
	return nil

}

// 处理完成的任务
func (c *Coordinator) processTaskResult(task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch task.TaskState {
	//  如果完成的任务是map任务，那么就将其生成的中间文件存储到master的内部，供后面的reduce使用
	case Map:
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		//  当全部任务都完成， 那么master就该更改状态为reduce阶段了
		if c.allTaskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	// reduce阶段，仅需判断任务是否全部完成，全部完成那么整个系统就完成了，设置为退出阶段即可
	case Reduce:
		if c.allTaskDone() {
			c.CoordinatorPhase = Exit
		}
	}
}
```

这里使用一个辅助函数判断是否当前阶段所有任务都已经完成

```go
// 所有任务都完成了
func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
```

**8. 转入Reduce阶段，worker获得ReduceTask，交给reducer处理**

```go
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
```

**9. master确认所有ReduceTask都已经完成，转入Exit阶段，终止所有master和worker goroutine**

```go
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 需要加锁，因为读的时候有其他线程在写
	c.mu.Lock()
	ret = c.CoordinatorPhase == Exit
	c.mu.Unlock()

	return ret
}
```


**10. 上锁**
master跟多个worker通信，master的数据是共享的，其中`TaskMeta, Phase, Intermediates, TaskQueue` 都有读写发生。`TaskQueue`使用`channel`实现，自己带锁。只有涉及`Intermediates, TaskMeta, Phase`的操作需要上锁。*PS.写的糙一点，那就是master每个方法都要上锁，master直接变成同步执行。。。*

另外go -race并不能检测出所有的datarace。我曾一度任务`Intermediates`写操作发生在map阶段，读操作发生在reduce阶段读，逻辑上存在`barrier`，所以不会有datarace. 但是后来想到两个write也可能造成datarace，然而Go Race Detector并没有检测出来。


**11. carsh处理**

test当中有容错的要求，不过只针对worker。mapreduce论文中提到了：

1. 周期性向worker发送心跳检测
+ 如果worker失联一段时间，master将worker标记成failed
+ worker失效之后
  + 已完成的map task被重新标记为idle
  + 已完成的reduce task不需要改变
  + 原因是：map的结果被写在local disk，worker machine 宕机会导致map的结果丢失；reduce结果存储在GFS，不会随着machine down丢失
2. 对于in-progress 且超时的任务，启动backup执行


tricky的在于Lab1是在单机上用多进程模拟了多机器，但并不会因为进程终止导致写好的文件丢失，这也是为什么我前面没有按照论文保留workerID.

针对Lab1修改后的容错设计：

1. 周期性检查task是否完成。将超时未完成的任务，交给新的worker，backup执行

```go
// 检查超时的worker
func (c *Coordinator) catchTimeout() {
	for {
		time.Sleep(5 * time.Second)
		c.mu.Lock()
		if c.CoordinatorPhase == Exit {
			c.mu.Unlock()
			return
		}
		for _, coordinatorTask := range c.TaskMeta {
			// 如果任务的状态是处理中， 并且处理时间超过了10s 那么就属于出错了
			if coordinatorTask.TaskStatus == InProgress && time.Since(coordinatorTask.StartTime) > time.Second*10 {
				c.TaskQueue <- coordinatorTask.TaskReference
				coordinatorTask.TaskStatus = Idle
			}
		}
		c.mu.Unlock()
	}
}
```

2. 从第一个完成的worker获取结果，将后序的backup结果丢弃

```go
if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
	// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
	return nil
}
m.TaskMeta[task.TaskNumber].TaskStatus = Completed
```

---

## 测试

开启race detector，直接执行官方脚本`test-mr.sh`

```bash
$ ./test-mr.sh        
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

简单测试

```shell
git clone git@github.com:nullptroot/mapReduce.git
cd mapReduce/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go pg-*.txt
```

另起一个或者多个shell

```shell
go run mrworker.go wc.so
```

