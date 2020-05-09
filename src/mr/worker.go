package mr

import (
	"fmt"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	// 创建WorkerTask

	wt := WorkerTask{
		mapf:    mapf,
		reducef: reducef,
	}

	for {
		wt.CallGetTask()
		if wt.Type == MAPTASK {
			wt.DoMapWork()
		} else if wt.Type == REDUCETASK {
			wt.DoReduceWork()
		} else if wt.Type == STOPTASK {
			break
		} else if wt.Type == WAITTASK {
			time.Sleep(300 * time.Millisecond)
		}
	}

	return
}

func (t *WorkerTask) CallGetTask() {
	args := EmptyArgs{}
	newTask := WorkerTask{}
	call("Master.CreateTask", &args, &newTask)
	// fmt.Printf("task: %v\n", newTask)

	t.MapId = newTask.MapId
	t.ReduceId = newTask.ReduceId
	t.Type = newTask.Type
	t.State = newTask.State
	t.FileName = newTask.FileName
	t.MapNum = newTask.MapNum
	t.ReduceNum = newTask.ReduceNum
	t.MapTaskCount = newTask.MapTaskCount
	t.ReduceTaskCount = newTask.ReduceTaskCount
}

func (t *WorkerTask) CallReportTask(isSuccess bool) {
	// fmt.Printf("task content: %v\n", t)
	args := ReportTaskArgs{
		Type:            t.Type,
		MapId:           t.MapId,
		ReduceId:        t.ReduceId,
		IsSuccess:       isSuccess,
		MapTaskCount:    t.MapTaskCount,
		ReduceTaskCount: t.ReduceTaskCount,
	}
	reply := ReportTaskReply{}

	// fmt.Printf("Report to master: %v\n", args)

	call("Master.HandleTaskReport", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
