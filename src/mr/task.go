package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type TaskType int

const (
	MAPTASK    TaskType = 0
	REDUCETASK TaskType = 1
	STOPTASK   TaskType = 2
	WAITTASK   TaskType = 3
)

type TaskState int

const (
	UNDO TaskState = iota
	PROCESSING
	FINISHED
)

type WorkerTask struct {
	MapId           int
	ReduceId        int
	Type            TaskType
	State           TaskState
	FileName        string // 执行任务的文件名称
	MapNum          int
	ReduceNum       int
	MapTaskCount    int
	ReduceTaskCount int
	mapf            func(string, string) []KeyValue
	reducef         func(string, []string) string
}

func (t *WorkerTask) DoMapWork() {
	file, err := os.Open(t.FileName)
	if err != nil {
		t.CallReportTask(false)
		log.Fatalf("cannot open %v", t.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		t.CallReportTask(false)
		log.Fatalf("cannot read %v", t.FileName)
	}
	file.Close()
	kva := t.mapf(t.FileName, string(content))

	intermediate := make([][]KeyValue, t.ReduceNum, t.ReduceNum)
	// 分区
	for _, kv := range kva {
		idx := ihash(kv.Key) % t.ReduceNum
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for idx := 0; idx < t.ReduceNum; idx++ {
		// 之所以idx到ReduceNum为止，是因为需要生成ReduceNum个中间文件提供给Reduce任务处理
		intermediateFileName := fmt.Sprintf("mr-%d-%d", t.MapId, idx)
		// 将内容写入到intermediateFile
		file, err := os.Create(intermediateFileName)
		if err != nil {
			t.CallReportTask(false)
			log.Fatalf("cannot create %v", intermediateFileName)
		}
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		file.Close()
	}
	// 向master报告
	t.CallReportTask(true)
}

func (t *WorkerTask) DoReduceWork() {
	// 这个map的key是单词，value是内容全为1的数组，代表该单词出现的次数
	kvsReduce := make(map[string][]string)
	for idx := 0; idx < t.MapNum; idx++ {
		// 之所以idx到MapNum为止，是因为只有这么多个文件
		intermediateFileName := fmt.Sprintf("mr-%d-%d", idx, t.ReduceId)
		content, err := ioutil.ReadFile(intermediateFileName)
		if err != nil {
			t.CallReportTask(false)
			log.Fatalf("read file %v failed", intermediateFileName)
			return
		}
		kvs := make([]KeyValue, 0)
		// 把对应文件中的key-value pair读入到kvs中
		err = json.Unmarshal(content, &kvs)
		if err != nil {
			t.CallReportTask(false)
			log.Fatalf("fail to unmarshal the data")
			return
		}
		// 将每个单词出现的记录聚合起来，准备统计
		for _, kv := range kvs {
			if _, ok := kvsReduce[kv.Key]; !ok {
				kvsReduce[kv.Key] = make([]string, 0)
			}
			kvsReduce[kv.Key] = append(kvsReduce[kv.Key], kv.Value)
		}
	}
	result := make([]string, 0)
	for key, val := range kvsReduce {
		output := t.reducef(key, val)
		result = append(result, fmt.Sprintf("%v %v\n", key, output))
	}
	outputFileName := fmt.Sprintf("mr-out-%d", t.ReduceId)
	err := ioutil.WriteFile(outputFileName, []byte(strings.Join(result, "")), 0644)
	if err != nil {
		t.CallReportTask(false)
		log.Fatalf("write result failed")
		return
	}
	t.CallReportTask(true)
}
