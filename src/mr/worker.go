package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

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
	for {
		time.Sleep(time.Second * 3)
		log.Println("In worker")
		task, err := ConnectToMasterNode()
		if err != nil {
			log.Println(err.Error())
		}
		if task.END {
			break
			// return
		}
		worker := WorkerMeta{TaskID: task.TaskId, TaskLocation: task.TaskLocation, Role: task.Role, MapperFunc: mapf, ReduceFunc: reducef}
		err = worker.PerformTask()
		if err != nil {
			log.Println("Failed to perform task")
			// tell to master
		}
	}

}

type WorkerMeta struct {
	TaskID       string
	TaskLocation string
	Role         string
	MapperFunc   func(string, string) []KeyValue
	ReduceFunc   func(string, []string) string
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
		w.MapperFunc(w.TaskLocation, string(content))
		// log.Println("work done %+v", kva)
		return nil
		//Write to intermediate file
		//Intermediate file location send to Master
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
