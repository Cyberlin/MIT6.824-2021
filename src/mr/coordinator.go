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
	CleanTimer         *time.Ticker
	NMapComplete       int
	NReduceComplete    int
	NMachine           int
	NMap               int
	NReduce            int
	MapTaskCh          []*Task
	ReduceTaskCh       []*Task
	files              []string
	AllDone            bool
	Redo               int
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
func (this *Coordinator) setMapComplete(task *Task) {
	this.mu.Lock()
	defer this.mu.Unlock()
	//logger.Infof("Set map task%d complete with taskId = %d\n", this.StartedMapTasks[taskId].Id, taskId)
	//if this.StartedMapTasks[task.Id] == nil {
	//	this.StartedMapTasks[task.Id] = &Task{
	//		WorkerId:         task.WorkerId,
	//		Id:               task.Id,
	//		Type:             task.Type,
	//		State:            task.State,
	//		MapInputFile:     task.MapInputFile,
	//		ReduceInputFiles: task.ReduceInputFiles,
	//	}
	//}

	this.StartedMapTasks[task.Id].State = COMPLETE
	this.StartedMapTasks[task.Id].ReduceInputFiles = task.ReduceInputFiles
	this.CleanTimer.Reset(10 * time.Second)
	this.NMapComplete++
}

func (this *Coordinator) setReduceComplete(task *Task) {
	this.mu.Lock()
	defer this.mu.Unlock()

	//logger.Infof("Set reduce task%d completer with taskId = %d\n", this.StartedReduceTasks[taskId].Id, taskId)
	//if this.StartedReduceTasks[task.Id] == nil {
	//	this.StartedReduceTasks[task.Id] = &Task{
	//		WorkerId:         task.WorkerId,
	//		Id:               task.Id,
	//		Type:             task.Type,
	//		State:            task.State,
	//		MapInputFile:     task.MapInputFile,
	//		ReduceInputFiles: task.ReduceInputFiles,
	//	}
	//}
	this.StartedReduceTasks[task.Id].State = COMPLETE
	this.CleanTimer.Reset(5 * time.Second)
	this.NReduceComplete++
}

func (this *Coordinator) doTaskComplete(msg *HeartBeat) {

	if msg.MapComplete == true {
		this.setMapComplete(&msg.Task)
	}
	if msg.ReduceComplete == true {
		this.setReduceComplete(&msg.Task)
	}
}
func (this *Coordinator) CleanMapTask() {
	this.mu.Lock()
	defer this.mu.Unlock()
	for i := 0; i < 20; i++ {
		fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	}
	for i := 0; i < len(this.StartedMapTasks); i++ {
		toIdleMapTask := this.StartedMapTasks[i]
		if toIdleMapTask.State != COMPLETE {

			//fmt.Printf("the lennnnn is %d\n", len(this.MapTaskCh))
			for i := range this.StartedMapTasks {
				fmt.Printf("%v\n", this.StartedMapTasks[i])
			}
			this.MapTaskCh = append(this.MapTaskCh, toIdleMapTask)
			//fmt.Printf("the lennnnn is %d\n", len(this.MapTaskCh))

		}
	}
}
func (this *Coordinator) CleanReduceTask() {
	this.mu.Lock()
	defer this.mu.Unlock()
	for i := 0; i < 20; i++ {
		fmt.Println("###################")
	}
	for i := 0; i < len(this.StartedReduceTasks); i++ {
		toIdleReduceTask := this.StartedReduceTasks[i]
		if toIdleReduceTask.State != COMPLETE {

			//fmt.Printf("the lennnnn is %d\n", len(this.MapTaskCh))
			//for i := range this.StartedReduceTasks {
			//	fmt.Printf("%v\n", this.StartedReduceTasks[i])
			//}
			this.ReduceTaskCh = append(this.ReduceTaskCh, toIdleReduceTask)
			//fmt.Printf("the lennnnn is %d\n", len(this.StartedReduceTasks))

		}
	}
}
func (this *Coordinator) IdleMapTask(workerId int) {
	log.Panicln("the LEN: --------------------->:", len(this.StartedMapTasks))
	for i := 0; i < len(this.StartedMapTasks); i++ {
		toIdleMapTask := this.StartedMapTasks[i]

		if toIdleMapTask != nil && toIdleMapTask.WorkerId == workerId {
			this.StartedMapTasks[i] = nil
			log.Panicln("Set REDO !!!!!!!!!!!!!!!!!!!!")
			this.Redo = toIdleMapTask.WorkerId
			//fmt.Printf("the workerId: %d, and this.workerId: %d\n", workerId, toIdleMapTask.WorkerId)
			if toIdleMapTask.State == COMPLETE {
				this.NMapComplete--
			}
			toIdleMapTask.State = IDLE

			//fmt.Printf("the lennnnn is %d\n", len(this.MapTaskCh))
			//for i := range this.StartedMapTasks {
			//	fmt.Printf("%v\n", this.StartedMapTasks[i])
			//}

			this.MapTaskCh = append(this.MapTaskCh, toIdleMapTask)
			for i := range this.MapTaskCh {
				fmt.Printf("THE ADD MAP TASK %v\n", *this.MapTaskCh[i])
				panic("add")
			}

		}
	}
}
func (this *Coordinator) IdleReduceTask(workerId int) {
	for i := 0; i < len(this.StartedReduceTasks); i++ {
		toIdleReduceTask := this.StartedReduceTasks[i]
		if toIdleReduceTask != nil && toIdleReduceTask.WorkerId == workerId && toIdleReduceTask.State != INPROGRESS {
			this.StartedReduceTasks[i] = nil
			//fmt.Printf("the workerId: %d, and this.workerId: %d\n", workerId, toIdleReduceTask.WorkerId)
			toIdleReduceTask.State = IDLE

			this.ReduceTaskCh = append(this.ReduceTaskCh, toIdleReduceTask)
		}
	}
}

func (this *Coordinator) SetTimer(workId int) {

	timeout := time.NewTimer(10 * time.Second)
	this.mu.Lock()
	var inc []*time.Timer
	if workId >= len(this.WorkerTimers) {
		inc = make([]*time.Timer, workId-len(this.WorkerTimers)+1)
	}
	this.WorkerTimers = append(this.WorkerTimers, inc...)
	this.WorkerTimers[workId] = timeout
	this.mu.Unlock()
	for {
		select {
		case <-timeout.C:
			this.mu.Lock()
			this.NMachine--
			fmt.Printf("worker%d is timeout\n", workId)
			log.Panicln("CCCCCCCCCCCCCCCCCCCCCOlin")
			this.IdleMapTask(workId)

			fmt.Printf("worker%d: NMAPCompelete%d,NMap:%d\n", workId, this.NMapComplete, this.NMap)

			this.IdleReduceTask(workId)
			fmt.Printf("worker%d: NReduceCom: %d\n", workId, this.NReduceComplete)

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
	//this.mu.Lock()
	//timeout := time.NewTimer(10 * time.Second)
	//var inc []*time.Timer
	//if reply.WorkerID >= len(this.WorkerTimers) {
	//	inc = make([]*time.Timer, reply.WorkerID-len(this.WorkerTimers)+1)
	//}
	//this.WorkerTimers = append(this.WorkerTimers, inc...)
	//this.WorkerTimers[reply.WorkerID] = timeout
	//timer := this.WorkerTimers[workerId]
	//if timer != nil {
	//	this.WorkerTimers[workerId].Reset(10 * time.Second)
	//}
	//this.mu.Unlock()
	//if timer == nil {
	//	go this.SetTimer(workerId)
	//}

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
	c.WorkerTimers = make([]*time.Timer, c.NMap)
	c.Redo = -1
	c.CleanTimer = time.NewTicker(5 * time.Second)
	go c.CleanZombieTask()
	c.mu.Lock()
	c.IdleMapTaskInit()
	c.mu.Unlock()

	c.server()
	return &c
}
func (this *Coordinator) CleanZombieTask() {
	for {
		select {
		case <-this.CleanTimer.C:

			go this.CleanMapTask()
			go this.CleanReduceTask()
		default:
			time.Sleep(250 * time.Millisecond)
		}
	}
}
func (this *Coordinator) IdleReduceTaskInit() {
	this.StartedReduceTasks = make([]*Task, this.NReduce)
	this.ReduceTaskCh = make([]*Task, 0)
	files := make(map[int][]string)
	for i := range this.StartedMapTasks {
		curTask := this.StartedMapTasks[i]
		if curTask.State != COMPLETE {
			log.Println("Map task are not all completed")
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
		this.ReduceTaskCh = append(this.ReduceTaskCh, reduceTask)
	}
}

func (this *Coordinator) IdleMapTaskInit() {
	this.MapTaskCh = make([]*Task, 0)
	for i := 0; i < this.NMap; i++ {
		mapTask := &Task{
			Id:           i,
			Type:         MAP,
			State:        IDLE,
			MapInputFile: this.files[i],
		}
		this.MapTaskCh = append(this.MapTaskCh, mapTask)
	}
	this.StartedMapTasks = make([]*Task, this.NMap)
	logger.Info("Init mapTask Channel")
	return
}

func (this *Coordinator) getMapTask(workerId int) (*Task, bool) {
	if len(this.MapTaskCh) == 0 {
		return nil, false
	}
	task := this.MapTaskCh[0]
	this.MapTaskCh = this.MapTaskCh[1:]
	task.State = INPROGRESS
	task.WorkerId = workerId

	this.StartedMapTasks[task.Id] = task

	return task, true
}
func (this *Coordinator) getReduceTask(workerId int) (*Task, bool) {
	if len(this.ReduceTaskCh) == 0 {
		return nil, false
	}
	task := this.ReduceTaskCh[0]
	this.ReduceTaskCh = this.ReduceTaskCh[1:]
	task.State = INPROGRESS
	task.WorkerId = workerId

	this.StartedReduceTasks[task.Id] = task

	return task, true
}
