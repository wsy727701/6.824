package mr

import (
	"fmt"
	"log"
 	"net/rpc"
	"hash/fnv"
	"encoding/json"
	"os"
	"io/ioutil"
	//"time"
	"strings"
)

func MapOutputName (map_id, reduce_id int) string {
	return fmt.Sprintf("mr-%d-%d", map_id, reduce_id)
}
func ReduceOutputName (reduce_id int) string {
	return fmt.Sprintf("mr-out-%d", reduce_id)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	id int
	mapf func (string, string) []KeyValue
	reducef func(string, []string) string
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
// rpc call
func (w *WorkerInfo) WorkerEnter () {
	//fmt.Printf("Worker: A New Worker Enter\n")
	args := WorkerArgs{}
	reply := WorkerReply{}
	flag := call("Master.GetNewWorker", &args, &reply)
	if !flag {
		log.Fatal("A new worker enter failed")
	}
	w.id = reply.Worker_id
	//fmt.Printf("Worker: A New Worker has been in\n")
}

func (w *WorkerInfo) GetTask () *TaskInfo {
	args := TaskArgs {
		Worker_id: w.id,
	}
	reply := TaskReply{}
	flag := call("Master.NewTask", &args, &reply) 
	if !flag {
		log.Fatal("A new task get failed")
	}
	return reply.Task
}

func (w *WorkerInfo) Report (task TaskInfo, stat bool) {
	//fmt.Printf("Worker: Enter sending report   taskid=%d\n", task.Id)
	args := ReportArgs {
		Process: task.Process,
		Task_id: task.Id,
		End: stat,
		Worker_id: w.id,
	}
	reply := ReportReply{}
	//fmt.Printf("Worker: Inter sending report   taskid=%d\n", task.Id)
	flag := call("Master.TaskReport", &args, &reply)
	if !flag {
		log.Fatal("Report task failed")
	}
	//fmt.Printf("Worker: Finish sending report   taskid=%d\n", task.Id)
}

// worker work
func (w *WorkerInfo) Work () {
	for {
		task := w.GetTask()
		if task == nil {
			continue
		}
		if task.Process == 0 {
			w.DoMap(*task)
		} else {
			w.DoReduce(*task)
		}
		//time.Sleep(Interval)
	}
} 

func (w *WorkerInfo) DoMap (task TaskInfo) {
	//fmt.Printf("Worker: MapTask: filename=%s, id=%d\n", task.File_name, task.Id)
	content, err := ioutil.ReadFile(task.File_name)
	if err != nil {
		w.Report(task, false)
		return
	}
	kvs := w.mapf(task.File_name, string(content))
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		reduce_id := ihash(kv.Key) % task.NReduce
		intermediate[reduce_id] = append(intermediate[reduce_id], kv)
	}
	//fmt.Printf("Worker: MapProcess here 1  :%d\n", task.Id)
	for reduce_id, kv_list := range intermediate {
		file_ptr, err := os.Create(MapOutputName(task.Id, reduce_id))
		if err != nil {
			log.Fatal("Worker: MapTask: creat MapOutputFile failure", err)
			w.Report(task, false)
			return
		}	
		enc := json.NewEncoder(file_ptr)
		for _, kv := range kv_list {
			err := enc.Encode(&kv)
			if err != nil {
				w.Report(task, false)
				return
			}
		}
		err = file_ptr.Close()
		if err != nil {
			w.Report(task, false)
			return
		}
	}
	//fmt.Printf("Worker: MapProcess here 3  :%d\n", task.Id)
	w.Report(task, true)
}
func (w *WorkerInfo) DoReduce (task TaskInfo) {
	//fmt.Printf("Worker: Process reduce begin\n")
	maps := make(map[string][]string)
	for map_id := 0; map_id < task.Nmap; map_id ++ {
		file_name := MapOutputName(map_id, task.Id)
		file_ptr, err := os.Open(file_name)
		if err != nil {
			w.Report(task, false)
			return
		}
		dec := json.NewDecoder(file_ptr)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	rec := make([]string, 0, 100)
	for key, value := range maps {
		rec = append(rec, fmt.Sprintf("%v %v\n", key, w.reducef(key, value)))
	}
	err := ioutil.WriteFile(ReduceOutputName(task.Id), []byte(strings.Join(rec, "")), 0600)
	if err != nil {
		w.Report(task, false)
		return
	}
	w.Report(task, true)
	//fmt.Printf("Worker: Process reduce end\n")
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	w := WorkerInfo {
		id: 0,
		mapf: mapf,
		reducef: reducef,
	}
	w.WorkerEnter()
	w.Work()
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