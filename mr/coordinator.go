package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)

type State int

// 主master的状态
const (
	Map    State = iota //当前系统处于map阶段
	Reduce              //当前系统处于reduce阶段
	Exit                //当前系统处于exit阶段 也就是所有任务都完成了
	Wait                //当前系统处于wait阶段
)

// 要处理的具体任务  包括Map Reduce任务
type Task struct {
	Input         string   //任务的输入文件
	TaskState     State    //任务的状态
	NReduce       int      //有几个Reduce
	TaskNumber    int      //任务id
	Intermediates []string //Map任务会生成中间文件
	Output        string   //reduce的输出
}

// master中包装任务的结构体
type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus //任务的状态
	StartTime     time.Time             //处理任务的起始时间
	TaskReference *Task                 //具体的任务结构
}

type Coordinator struct {
	TaskQueue        chan *Task               // 任务队列  后面是一个有缓冲的chan
	TaskMeta         map[int]*CoordinatorTask //管理所有的任务map，使用任务id映射具体任务
	CoordinatorPhase State                    //master的状态
	NReduce          int                      //多少个reduce
	InputFiles       []string                 //任务的输入文件
	Intermediates    [][]string               //map产生的中间文件
	mu               sync.Mutex               //锁来控制互斥
}

// type Coordinator struct {
// 	// Your definitions here.

// }

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// go语言rpc函数的示例
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 下面就是使用http协议实现rpc服务
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	//注册对象c的rpc服务
	rpc.Register(c)
	//设置http的路由
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// 获取本地socket通信的文件名
	sockname := coordinatorSock()
	// 避免文件存在，先删除
	os.Remove(sockname)
	// 创建本地socket通信
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 开始监听
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 需要加锁，因为读的时候有其他线程在写
	c.mu.Lock()
	ret = c.CoordinatorPhase == Exit
	c.mu.Unlock()

	return ret
}
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
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

// 创建map任务
func (c *Coordinator) CreateMapTask() {
	for idx, filename := range c.InputFiles {
		// 具体的任务
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReduce:    c.NReduce,
			TaskNumber: idx,
		}
		// 发送给Coordinator的TaskQueue
		c.TaskQueue <- &taskMeta
		// Coordinator管理Task
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle, // 刚开始任务处于空闲的状态
			TaskReference: &taskMeta,
		}
	}
}

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

// 所有任务都完成了
func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

// 创建reduce任务
func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask)
	for idx, files := range c.Intermediates {
		task := Task{
			TaskState:     Reduce,
			NReduce:       c.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		c.TaskQueue <- &task
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &task,
		}
	}
}
