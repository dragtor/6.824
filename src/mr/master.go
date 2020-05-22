package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	ALLOCATED   string = "ALLOCATED"
	UNALLOCATED string = "UNALLOCATED"
	IDEAL       string = "IDEAL"
	INPROGRESS  string = "INPROGRESS"
)

type Master struct {
	TaskList  []*Task
	Processes map[string]*ProcessMeta
}

type Task struct {
	TaskId       string
	FileName     string
	TaskLocation string
	Status       string // ALLOCATED , UNALLOCATED
}

type ProcessMeta struct {
	TaskDetails *Task
	Status      string
	StartTime   int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AssignTask(request *WorkerRequest, response *MasterResponse) error {
	// checks worker status
	// If worker status == IDEAL ==> assign new task to worker to map function on input data
	// iff master already noted all task done then worker will treat as reducer
	if request.Status == IDEAL {
		task := m.GetUnallocatedTask()
		if task == nil {
			response.END = true
			return nil
		}
		m.UpdateTaskInProcessingList(task)
		response.TaskLocation = task.TaskLocation
		return nil
	}
	return nil
}

func (m *Master) UpdateTaskInProcessingList(task *Task) {
	currentTime := makeTimestamp()
	if _, present := m.Processes[task.TaskId]; !present {
		if m.Processes == nil {
			m.Processes = make(map[string]*ProcessMeta)
		}
		log.Printf("taskId not present")
		m.Processes[task.TaskId] = &ProcessMeta{TaskDetails: task, Status: INPROGRESS, StartTime: currentTime}
	}
	m.Processes[task.TaskId] = &ProcessMeta{TaskDetails: task, Status: INPROGRESS, StartTime: currentTime}
}

//GetUnallocatedTask This method will return unallocated task from taskList
func (m *Master) GetUnallocatedTask() *Task {
	for _, task := range m.TaskList {
		if task.Status == UNALLOCATED {
			task.Status = ALLOCATED
			return task
		}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.GenerateTaskList(files)
	m.server()
	return &m
}

func (m *Master) GenerateTaskList(files []string) {
	log.Println("Processing tasks into subtasks")
	for _, file := range files {
		task := &Task{FileName: file, TaskLocation: file, Status: UNALLOCATED}
		m.TaskList = append(m.TaskList, task)
	}
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
