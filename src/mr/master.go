package mr

import (
	"log"
	"net"
	"os"
	//"fmt"
	"net/rpc"
	"net/http"
	"sync"
	"time" 
)

const (
	TaskWaiting = 0
	TaskQueueing = 1
	TaskRunning = 2
	TaskFinished = 3
	TaskError = 4
)

const (
	RunTimeLimit = 5 * time.Second 
	Interval = 500 * time.Millisecond  
)

type TaskInfo struct {
	Process int
	Nmap int
	Id int
	NReduce int
	File_name string
}

type TaskStatus struct {
	begin time.Time
	worker_id int
	stat int
}

type Master struct {
	// Your definitions here.
	st_tasks []TaskStatus
	files []string
	end bool
	nReduce int
	cnt_worker int
	process int //   0 for map        1 for reduce
	multi sync.Mutex
	ch_task chan TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetNewWorker (args *WorkerArgs, reply *WorkerReply) error {
	//fmt.Printf("Master: A new worker enter\n")
	m.multi.Lock() // blocked here ??
	defer m.multi.Unlock()
	m.cnt_worker++
	reply.Worker_id = m.cnt_worker
	//fmt.Printf("Master: A new worker has been in\n")
	//fmt.Println(*reply)
	return nil	
}

func (m *Master) NewTask (args *TaskArgs, reply *TaskReply) error {
	//fmt.Printf("Master: A new task enter\n")
	task := <- m.ch_task
	reply.Task = &task
	m.multi.Lock()
	defer m.multi.Unlock()
	if task.Process != m.process {
		reply.Task = nil
		return nil
	}
	m.st_tasks[task.Id].begin = time.Now()
	m.st_tasks[task.Id].stat = TaskRunning
	m.st_tasks[task.Id].worker_id = args.Worker_id
	//fmt.Printf("Master: A new task has been established\n")
	//fmt.Println(task)
	return nil
}

func (m *Master) TaskReport (args *ReportArgs, reply *ReportReply) error {
	//fmt.Printf("Master: New report enter -----taskid=%d\n", args.Task_id)
	m.multi.Lock()
	defer m.multi.Unlock()
	if args.Process != m.process || args.Worker_id != m.st_tasks[args.Task_id].worker_id {
		return nil
	}
	if args.End {
		m.st_tasks[args.Task_id].stat = TaskFinished
	} else {
		m.st_tasks[args.Task_id].stat = TaskError	
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	m.multi.Lock()
	defer m.multi.Unlock()
	ret := false
	if m.end {
		ret = true 
	}
	return ret
}

// get a new task 
func (m *Master) getTask (index int) TaskInfo {
	task := TaskInfo {
		Process: m.process,
		Nmap: len(m.files),
		Id: index,
		NReduce: m.nReduce,
		File_name: "",
	}
	if m.process == 0 {
		task.File_name = m.files[index]
	}
	//fmt.Printf("Master: Task: id=%d, filename=%s, tasktype=%d\n", task.Id, task.File_name, task.Process)
	return task
}

// arrange tasks
func (m *Master) taskArrange () {
 	m.multi.Lock()
	defer m.multi.Unlock()
	if m.end {
		return
	}
	//fmt.Printf("Master: Begin arrange tasks\n")
	well_done := true
	for id, task := range m.st_tasks {
		//fmt.Println(task)
		switch task.stat {
			case TaskWaiting :
				well_done = false
				m.ch_task <- m.getTask(id)
				m.st_tasks[id].stat = TaskQueueing
			case TaskQueueing :
				well_done = false
			case TaskRunning :
				well_done = false
				if time.Now().Sub(task.begin) > RunTimeLimit {
					m.st_tasks[id].stat = TaskQueueing
					m.ch_task <- m.getTask(id)
				}
			case TaskFinished :
			case TaskError :
				well_done = false
				m.st_tasks[id].stat = TaskQueueing
				m.ch_task <- m.getTask(id)
		}
	}
	if well_done {
		if m.process == 0 {
			m.process = 1
			//fmt.Print("Process map finished\n")
			m.mInitialize()
		} else {
			m.end = true
			nmap := len(m.files)
			for i := 0; i < m.nReduce; i++ {
				for j := 0; j < nmap; j++{
					os.Remove(MapOutputName(j, i))
				}
			}
		}
	}
	//fmt.Printf("Master: End arrange tasks\n")
}

func (m *Master) arrangeTasks () {
	for !m.Done() {
		go m.taskArrange()
		time.Sleep(Interval)
	}
}

// initialize struct master
func (m *Master) valueInit (files []string, nReduce int) {
	m.nReduce = nReduce
	m.files = files
	m.multi = sync.Mutex{}
	m.process = 0
	m.end = false
	var bigger int
	files_length := len(files)
	if files_length > nReduce {
		bigger = files_length
	} else {
		bigger = nReduce
	}
	m.ch_task = make(chan TaskInfo, bigger)
}

// initialize a new process (map or reduce)
func (m *Master) mInitialize () {
	var length int
	switch m.process {
		case 0 : length = len(m.files)
		case 1 : length = m.nReduce
	}
	m.st_tasks = make([]TaskStatus, length)
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}	
	// Your code here.				
	m.valueInit(files, nReduce)
	m.mInitialize()
	go m.arrangeTasks()
	m.server()	
	return &m
}