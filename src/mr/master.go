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

type Master struct {
	// Your definitions here.
	mu               sync.Mutex
	MapAllDone       bool
	ReduceAllDone    bool
	files            []string
	MapTaskNum       int
	ReduceTaskNum    int
	MapTasks         []TaskState
	ReduceTasks      []TaskState
	MapTaskCounts    []int // 记录MapTask执行次数，只有最后一次执行的提交才被认为是有效的
	ReduceTaskCounts []int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("received rpc request from worker!")
	return nil
}

// 处理worker申请任务的函数
func (m *Master) CreateTask(args *EmptyArgs, task *WorkerTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task.ReduceNum = m.ReduceTaskNum
	task.MapNum = m.MapTaskNum
	if !m.MapAllDone {
		// Map任务没有全部做完，返回Map任务
		for i := 0; i < m.MapTaskNum; i++ {
			if m.MapTasks[i] == UNDO {
				task.MapId = i
				task.Type = MAPTASK
				task.FileName = m.files[i]
				m.MapTasks[i] = PROCESSING
				m.MapTaskCounts[i]++
				task.MapTaskCount = m.MapTaskCounts[i]
				return nil
			}
		}
		task.Type = WAITTASK
		return nil
	}
	if !m.ReduceAllDone {
		// Reduce任务没有全部做完，返回Reduce任务
		for i := 0; i < m.ReduceTaskNum; i++ {
			if m.ReduceTasks[i] == UNDO {
				task.ReduceId = i
				task.Type = REDUCETASK
				m.ReduceTasks[i] = PROCESSING
				m.ReduceTaskCounts[i]++
				task.ReduceTaskCount = m.ReduceTaskCounts[i]
				return nil
			}
		}
		task.Type = WAITTASK
		return nil
	}

	task.Type = STOPTASK
	return nil
}

func (m *Master) HandleTaskReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.IsSuccess {
		if args.Type == MAPTASK {
			//fmt.Printf("arg count: %d, map count: %d\n", args.MapTaskCount, m.MapTaskCounts[args.MapId])
			if args.MapTaskCount == m.MapTaskCounts[args.MapId] {
				m.MapTasks[args.MapId] = FINISHED
			}
		} else if args.Type == REDUCETASK {
			if args.ReduceTaskCount == m.ReduceTaskCounts[args.MapId] {
				m.ReduceTasks[args.ReduceId] = FINISHED
			}
		}
	} else {
		if args.Type == MAPTASK {
			if m.MapTasks[args.MapId] != FINISHED {
				m.MapTasks[args.MapId] = UNDO
			}
		} else if args.Type == REDUCETASK {
			if m.ReduceTasks[args.ReduceId] != FINISHED {
				m.ReduceTasks[args.ReduceId] = UNDO
			}
		}
	}

	// 检查任务是否全部完成
	for idx := 0; idx < m.MapTaskNum; idx++ {
		if m.MapTasks[idx] != FINISHED {
			break
		} else {
			if idx == m.MapTaskNum-1 {
				m.MapAllDone = true
			}
		}
	}

	for idx := 0; idx < m.ReduceTaskNum; idx++ {
		if m.ReduceTasks[idx] != FINISHED {
			break
		} else {
			if idx == m.ReduceTaskNum-1 {
				m.ReduceAllDone = true
			}
		}
	}
	return nil
}

func (m *Master) HandleTimeout(args *EmptyArgs, reply *EmptyReply) error {
	for {
		m.mu.Lock()
		if m.MapAllDone && m.ReduceAllDone {
			m.mu.Unlock()
			break
		}
		time.Sleep(100 * time.Millisecond)
		if !m.MapAllDone {
			for idx := 0; idx < m.MapTaskNum; idx++ {
				if m.MapTasks[idx] == PROCESSING {
					m.MapTasks[idx] = UNDO
				}
			}
		} else {
			for idx := 0; idx < m.ReduceTaskNum; idx++ {
				if m.ReduceTasks[idx] == PROCESSING {
					m.ReduceTasks[idx] = UNDO
				}
			}
		}
		m.mu.Unlock()
		time.Sleep(2000 * time.Millisecond)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.MapAllDone && m.ReduceAllDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		MapAllDone:       false,
		ReduceAllDone:    false,
		files:            files,
		MapTaskNum:       len(files),
		ReduceTaskNum:    nReduce,
		MapTasks:         make([]TaskState, len(files)),
		ReduceTasks:      make([]TaskState, nReduce),
		MapTaskCounts:    make([]int, len(files)),
		ReduceTaskCounts: make([]int, nReduce),
	}

	// Your code here.

	m.server()
	args, reply := EmptyArgs{}, EmptyReply{}
	go m.HandleTimeout(&args, &reply)
	return &m
}
