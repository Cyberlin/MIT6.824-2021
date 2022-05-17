package mr

import (
	"6.824/logging"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var logger logging.Logger = logging.NewSimpleLogger()

// an example RPC handler.

func (this *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (this *Coordinator) server() {
	logger.Infoln("Master starts to run server")
	rpc.Register(this)
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
func (this *Coordinator) Done() bool {
	ret := false

	// Your code here.
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.AllDone == true {
		ret = true
	}

	return ret
}

type Coordinator struct {
	// Your definitions here.
	mu                 sync.Mutex
	StartedMapTasks    []*Task
	StartedReduceTasks []*Task
	WorkerTimers       []*time.Timer
	NMapComplete       int
	NReduceComplete    int
	NMachine           int
	NMap               int
	NReduce            int
	MapTaskCh          chan *Task
	ReduceTaskCh       chan *Task
	files              []string
	AllDone            bool
}

//type TaskManager struct {
//	NMapComplete     int
//	NReduceComplete  int
//	IdleMapTaskCh    chan *Task
//	IdleReduceTaskCh chan *Task
//	StartedTasks     map[int][2][]*Task
//}

func (this *Coordinator) newWorkerID() int {
	this.mu.Lock()
	defer this.mu.Unlock()
	workerID := this.NMachine
	this.NMachine++
	return workerID
}
func (this *Coordinator) AssignTask(workerId int) *Task {
	logger.Infoln("Assign a new task")
	this.mu.Lock()
	defer this.mu.Unlock()

	task, ok := this.getMapTask(workerId)
	if !ok {
		//没有maptask 了, 说明全部分配完了
		logger.Infoln("All map task is run out")
		if this.NMapComplete == this.NMap {
			logger.Infoln("All map tasks are done")

			if this.ReduceTaskCh == nil {
				logger.Infoln("Idle reduce task init")
				this.IdleReduceTaskInit()
			}

			task, ok = this.getReduceTask(workerId)
			if !ok {
				//没有reduceWork了,说明分配完了,等待最后alldone
				logger.Infoln("All reduce work is run out")
				if this.NReduceComplete == this.NReduce {
					logger.Infoln("All reduce works are done")
					this.AllDone = true

					logger.Infoln("Get Done work")

					return &Task{Type: DONE}
				} else {

					return &Task{Type: WAIT, Id: 9999}
				}

			}
			logger.Infoln("Get a reduce task")
			return task

		} else {

			return &Task{Type: WAIT, Id: 6612}
		}

	}
	logger.Infof("Get a map task%d with file : %s\n", task.Id, task.MapInputFile)
	return task
}
func (this *Coordinator) setMapComplete(taskId int, files []string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	logger.Infof("Set map task%d complete with taskId = %d\n", this.StartedMapTasks[taskId].Id, taskId)
	this.StartedMapTasks[taskId].State = COMPLETE
	this.StartedMapTasks[taskId].ReduceInputFiles = files
	this.NMapComplete++
}

func (this *Coordinator) setReduceComplete(taskId int) {
	this.mu.Lock()
	defer this.mu.Unlock()

	logger.Infof("Set reduce task%d completer with taskId = %d\n", this.StartedReduceTasks[taskId].Id, taskId)
	this.StartedReduceTasks[taskId].State = COMPLETE
	this.NReduceComplete++
}

func (this *Coordinator) doTaskComplete(msg *HeartBeat) {

	taskId := msg.Task.Id

	if msg.MapComplete == true {
		this.setMapComplete(taskId, msg.Task.ReduceInputFiles)
	}
	if msg.ReduceComplete == true {
		this.setReduceComplete(taskId)
	}
}
func (this *Coordinator) IdleMapTask(workerId int) {
	for i := 0; i < len(this.StartedMapTasks); i++ {
		toIdleMapTask := this.StartedMapTasks[i]
		if toIdleMapTask.WorkerId == workerId {
			fmt.Printf("the workerId: %d, and this.workerId: %d\n", workerId, toIdleMapTask.WorkerId)
			if toIdleMapTask.State == COMPLETE {
				this.NMapComplete--
			}
			toIdleMapTask.State = IDLE
			this.MapTaskCh <- toIdleMapTask
		}
	}
}
func (this *Coordinator) IdleReduceTask(workerId int) {
	for i := 0; i < len(this.StartedReduceTasks); i++ {
		toIdleReduceTask := this.StartedReduceTasks[i]
		if toIdleReduceTask.WorkerId == workerId {
			fmt.Printf("the workerId: %d, and this.workerId: %d\n", workerId, toIdleReduceTask.WorkerId)
			toIdleReduceTask.State = IDLE
			this.MapTaskCh <- toIdleReduceTask
		}
	}
}
func (this *Coordinator) SetTimer(workId int) {

	timeout := time.NewTimer(5 * time.Second)
	this.mu.Lock()
	this.WorkerTimers[workId] = timeout
	this.mu.Unlock()
	for {
		select {
		case <-timeout.C:
			this.mu.Lock()
			fmt.Printf("worker%d is timeout\n", workId)

			fmt.Printf("worker%d: before StartMap: %v\n", workId, this.StartedMapTasks)
			this.IdleMapTask(workId)
			fmt.Printf("worker%d: after StartMap: %v\n", workId, this.StartedMapTasks)

			fmt.Printf("worker%d: NMAPCompelete%d,NMap:%d\n", workId, this.NMapComplete, this.NMap)

			this.IdleReduceTask(workId)
			fmt.Printf("worker%d: NReduceCom: %d\n", workId, this.NReduceComplete)
			time.Sleep(5 * time.Second)
			//this.WorkerTimers[workId].Stop()
			this.mu.Unlock()

		}
	}

}

// Your code here -- RPC handlers for the worker to call.
func (this *Coordinator) DoHeartBeat(args *HeartBeat, reply *HeartBeat) error {

	recv := args
	this.doTaskComplete(recv)
	workerId := recv.WorkerID
	if recv.WorkerID == 0 {
		logger.Infoln("Register a new workerId")
		reply.WorkerID = this.newWorkerID()
		workerId = reply.WorkerID
	} else {
		reply.WorkerID = recv.WorkerID
	}
	this.mu.Lock()
	timer := this.WorkerTimers[workerId]
	this.mu.Unlock()
	if timer != nil {
		this.WorkerTimers[workerId].Reset(5 * time.Second)
	}
	if timer == nil {
		go this.SetTimer(workerId)
	}

	//分配一个任务
	reply.Task = *this.AssignTask(workerId)

	logger.Infof("reply.Task%d workerID %dwith file %s\n", reply.Task.Id, reply.WorkerID, reply.Task.MapInputFile)

	reply.NReduce = this.NReduce

	return nil

	//if workerMsg.MachineIsIdle == true {
	//	task, ok := this.getMapTask()
	//	if !ok {
	//		logger.Warn("no more maptask")
	//		this.mu.Lock()
	//		defer this.mu.Unlock()
	//		this.AllDone = true
	//		reply.Task = Task{Type: NONE}
	//		return nil
	//	}
	//	logger.Infof("Get Map task%d , fileinput: %s", task.Id, task.MapInputFile)
	//
	//	reply.Task = *task
	//	reply.NReduce = this.NReduce
	//}
	//return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.NMap = len(files)
	c.NReduce = nReduce
	c.NMachine = 1
	c.files = files
	c.WorkerTimers = make([]*time.Timer, c.NMap+1)
	c.IdleMapTaskInit()

	c.server()
	return &c
}

func (this *Coordinator) IdleReduceTaskInit() {

	this.ReduceTaskCh = make(chan *Task, this.NReduce)
	files := make(map[int][]string)
	for i := range this.StartedMapTasks {
		curTask := this.StartedMapTasks[i]
		if curTask.State != COMPLETE {
			log.Panicln("Map task are not all completed")
		}
		for _, item := range curTask.ReduceInputFiles {
			strs := strings.Split(item, "-")
			reduceTaskId, err := strconv.Atoi(strs[len(strs)-1])
			if err != nil {
				log.Panicln("err:", err)
			}

			files[reduceTaskId] = append(files[reduceTaskId], item)
		}
		fmt.Printf("files[2]: %v\n", files[2])

	}
	for i := 0; i < this.NReduce; i++ {
		reduceTask := &Task{
			Id:               i,
			Type:             REDUCE,
			State:            IDLE,
			ReduceInputFiles: files[i],
		}
		this.ReduceTaskCh <- reduceTask
	}
}
func (this *Coordinator) IdleMapTaskInit() {
	this.MapTaskCh = make(chan *Task, this.NMap)
	for i := 0; i < this.NMap; i++ {
		mapTask := &Task{
			Id:           i,
			Type:         MAP,
			State:        IDLE,
			MapInputFile: this.files[i],
		}
		this.MapTaskCh <- mapTask
	}
	logger.Info("Init mapTask Channel")
	return
}

func (this *Coordinator) getMapTask(workerId int) (*Task, bool) {
	if len(this.MapTaskCh) == 0 {
		return nil, false
	}
	task := <-this.MapTaskCh
	task.State = INPROGRESS
	task.WorkerId = workerId
	if task.Id > len(this.StartedMapTasks)-1 {
		this.StartedMapTasks = append(this.StartedMapTasks, task)
	} else {
		this.StartedMapTasks[task.Id] = task
	}
	return task, true
}
func (this *Coordinator) getReduceTask(workerId int) (*Task, bool) {
	if len(this.ReduceTaskCh) == 0 {
		return nil, false
	}
	task := <-this.ReduceTaskCh
	task.State = INPROGRESS
	task.WorkerId = workerId
	if task.Id > len(this.StartedReduceTasks)-1 {
		this.StartedReduceTasks = append(this.StartedReduceTasks, task)
	} else {
		this.StartedReduceTasks[task.Id] = task
	}
	return task, true
}
