package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
	for {
		time.Sleep(time.Second * 1)
		log.Println("In worker")
		task, err := ConnectToMasterNode()
		if err != nil {
			log.Println(err.Error())
		}
		if task.END {
			break
		}
		Job := WorkerMeta{TaskID: task.TaskId, TaskLocation: task.TaskLocation, Role: task.Role, MapperFunc: mapf, ReduceFunc: reducef, NReduce: task.ReducerCount,
			IntermediateData: make(map[string][]KeyValue, task.ReducerCount)}
		err = Job.PerformTask()
		if err != nil {
			log.Println("Failed to perform task")
			// tell to master
		}
		intermediateFileData := Job.GetIntermediateFileData()
		resp, err := UpdateIntermediateFileDataToMasterNode(intermediateFileData)
		if err != nil {
			log.Println("Failed to perform task")
		}
		if resp.END {
			continue
		}
	}

}

func UpdateIntermediateFileDataToMasterNode(files []string) (*MasterResponse, error) {
	args := WorkerRequest{Status: COMPLETED}
	reply := MasterResponse{}
	resptl := call("Master.RegisterIntermediateJobData", &args, &reply)
	if !resptl {
		log.Println("Failed to Call RPC")
		return nil, errors.New("Failed To Call RPC Master.AssignTask")
	}
	log.Println("worker reply from master %+v", reply)
	return &reply, nil
}

type WorkerMeta struct {
	TaskID           string
	TaskLocation     string
	Role             string
	NReduce          int
	MapperFunc       func(string, string) []KeyValue
	ReduceFunc       func(string, []string) string
	IntermediateData map[string][]KeyValue //map[fileName] []KeyValue
}

func (w *WorkerMeta) GetIntermediateFileData() []string {
	var files []string
	for file, _ := range w.IntermediateData {
		files = append(files, file)
	}
	return files
}

//PerformTask ...
func (w *WorkerMeta) PerformTask() error {
	if w.Role == MAPPER {
		log.Println("Performing Mapper function")
		file, err := os.Open(w.TaskLocation)
		if err != nil {
			log.Fatalf("cannot open %v", w.TaskLocation)
			return errors.New("cannot open ")
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", w.TaskLocation)
			return errors.New("cannot read")
		}
		file.Close()

		kva := w.MapperFunc(w.TaskLocation, string(content))
		for _, pair := range kva {
			intermediateFileId := ihash(pair.Key) % w.NReduce
			fileName := "mr_" + w.TaskID + "_" + strconv.Itoa(intermediateFileId)
			w.IntermediateData[fileName] = append(w.IntermediateData[fileName], pair)
		}

		for fileName, _ := range w.IntermediateData {
			log.Println(fileName)
			file, _ := os.OpenFile(fileName, os.O_CREATE, os.ModePerm)
			defer file.Close()
			data, _ := w.IntermediateData[fileName]
			//optimize write to file using json.encode
			intermediateJson, err := json.Marshal(data)
			if err != nil {
				return err
			}
			err = ioutil.WriteFile(fileName, intermediateJson, 0644)
			if err != nil {
				return err
			}
		}
		return nil
	} else if w.Role == REDUCER {
		return nil
	}
	return nil
}

//ConnectToMasterNode ...
func ConnectToMasterNode() (*MasterResponse, error) {
	args := WorkerRequest{Status: IDEAL}
	reply := MasterResponse{}
	resptl := call("Master.AssignTask", &args, &reply)
	if !resptl {
		log.Println("Failed to Call RPC")
		return nil, errors.New("Failed To Call RPC Master.AssignTask")
	}
	log.Println("worker reply from master %+v", reply)
	return &reply, nil
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
