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

var HeartMsg = HeartBeat{}
var WorkerInfo = struct {
	WorkerId int
	Mapf     func(string, string) []KeyValue
	Reducef  func(string, []string) string
}{}

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

	// Your worker implementation here.
	workerInit(mapf, reducef)
	//不断传递心跳信息,获取心跳任务类型

	for {
		msg := doHeartBeat()

		taskType := msg.Task.Type
		logger.Infof("worker%d: get the task:%d\n ", WorkerInfo.WorkerId, msg.Task.Id)

		switch taskType {
		case MAP:
			doMap()
		case REDUCE:
			doReduce()
		case WAIT:
			logger.Infof("worker%d wait 25ms\n", WorkerInfo.WorkerId)
			time.Sleep(25 * time.Millisecond)
		case DONE:
			goto Done
		default:
			log.Panic("Unknow task type")
		}
	}
Done:
	fmt.Println("All done")

}
func workerInit(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	WorkerInfo.Mapf = mapf
	WorkerInfo.Reducef = reducef
}
func doHeartBeat() *HeartBeat {
	//与master 进行通信,从master 获取相应的心跳信息,心跳信息中包含了任务,rpc 调用
	HeartMsg.MachineIsIdle = true
	args := HeartMsg
	reply := HeartBeat{}
	logger.Infoln("Worker send heart beat")
	call("Coordinator.DoHeartBeat", &args, &reply)
	HeartMsg = reply
	WorkerInfo.WorkerId = reply.WorkerID
	fmt.Println("the workerId is :", HeartMsg.WorkerID, "___: ", reply.WorkerID)
	return &reply
}
func GenerateIntermediatePairs(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	logger.Infof("Read the %s: \n", filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}
func doMap() {
	logger.Infof("worker%d: Map task %d starts file : %s\n", WorkerInfo.WorkerId, HeartMsg.Task.Id, HeartMsg.Task.MapInputFile)

	mapTaskId := HeartMsg.Task.Id
	filename := HeartMsg.Task.MapInputFile
	nReduce := HeartMsg.NReduce
	mapf := WorkerInfo.Mapf

	intermediate := GenerateIntermediatePairs(filename, mapf)

	tempFiles := make([]*os.File, nReduce)
	tempFileNames := make([]string, nReduce)
	//创建中间文件
	for i := 0; i < nReduce; i++ {
		tempFileName := fmt.Sprintf("mr-%d-%d", mapTaskId, i)
		newFile, err := os.OpenFile(tempFileName, os.O_WRONLY|os.O_RDONLY|os.O_CREATE, 0777)
		if err != nil {
			logger.Fatal("can't create file: ", tempFileName)
		}
		logger.Infof("worker%d :Create intermediate file: %s\n", WorkerInfo.WorkerId, tempFileName)
		//
		tempFiles[i] = newFile
		tempFileNames[i] = tempFileName
	}

	// 写入键值对
	for _, pair := range intermediate {
		reduceId := ihash(pair.Key) % nReduce

		curFile := tempFiles[reduceId]

		err := json.NewEncoder(curFile).Encode(&pair)
		if err != nil {
			logger.Fatalln(err)
		}
	}
	//// rename
	//for i := 0; i < nReduce; i++ {
	//	newPath := fmt.Sprintf("mr-%d-%d", mapTaskId, i)
	//	os.Rename(tempFileNames[i], newPath)
	//	tempFileNames[i] = newPath
	//	tempFiles[i].Close()
	//}

	recordMapComplete(tempFileNames)
}
func recordMapComplete(files []string) {
	fmt.Printf("worker%d :map task complete\n", WorkerInfo.WorkerId)
	HeartMsg.MapComplete = true
	HeartMsg.Task.ReduceInputFiles = files
}
func ReadIntermediatePairs(filenames []string) []KeyValue {
	var intermediate []KeyValue

	for _, filename := range filenames {
		file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
		if err != nil {
			logger.Infoln(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	return intermediate
}
func doReduce() {
	fmt.Printf("worker%d :do reduce\n", WorkerInfo.WorkerId)

	filenames := HeartMsg.Task.ReduceInputFiles
	reduceTaskId := HeartMsg.Task.Id
	reducef := WorkerInfo.Reducef
	intermediate := ReadIntermediatePairs(filenames)
	logger.Infof("worker%d: Reduce reads all the pairs for middle file\n", WorkerInfo.WorkerId)

	//键值对排序
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 创建输入文件
	outputFileName := fmt.Sprintf("mr-tmp-%d", reduceTaskId)
	outputFile, err := os.OpenFile(outputFileName, os.O_RDONLY|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		logger.Fatalln(err)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	err = os.Rename(outputFileName, fmt.Sprintf("mr-out-%d", reduceTaskId))
	if err != nil {
		logger.Fatalln(err)
	}
	outputFile.Close()

	recordReduceComplete()
}
func recordReduceComplete() {
	fmt.Printf("worker%d: reduce task complete\n", WorkerInfo.WorkerId)
	HeartMsg.ReduceComplete = true
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
	args.X = 6612

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
