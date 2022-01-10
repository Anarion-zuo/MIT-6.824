package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

// import "math/rand"
import "os"
import "io"
// import "time"
import "encoding/json"
import "sort"

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

func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}

const pathPrefix = "./"

func makePgFileName(name *string) string {
	return "pg-" + *name + ".txt"
}

func makeIntermediateFromFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	// path := "../" + filename
	// path := fmt.Sprintf("../%v", filename)
	path := filename
	// test script manages relative path structure
	// open file
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	// read file content
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	file.Close()
	// generate kv array
	kva := mapf(filename, string(content))
	return kva
}

type AWorker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	// false on map
	// true on reduce
	mapOrReduce bool

	// must exit if true
	allDone bool

	workerId int
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// time.Sleep(1000 * time.Millisecond)

	// Your worker implementation here.

	worker := AWorker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.mapOrReduce = false
	worker.allDone = false
	worker.workerId = -1

	worker.logPrintf("initialized!\n")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// main process
	for !worker.allDone {
		// wait for a random little while
		// rand.Seed(time.Now().UnixNano())
		// n := rand.Intn(10)
		// log.Printf("wait for %vms\n", n *100)
		// time.Sleep(time.Duration(1000) * time.Millisecond)
		worker.process()
	}
	worker.logPrintf("no more tasks, all done, exiting...\n")
}

func (worker *AWorker) logPrintf(format string, vars ...interface{}) {
	log.Printf("worker %d: "+format, worker.workerId, vars)
}

func (worker *AWorker) askMapTask() *MapTaskReply {
	args := MapTaskArgs{}
	args.WorkerId = worker.workerId
	reply := MapTaskReply{}

	worker.logPrintf("requesting for map task...\n")
	call("Coordinator.GiveMapTask", &args, &reply)
	// worker.logPrintf("request for map task received...\n")

	// obtain a worker id no matter what
	worker.workerId = reply.WorkerId

	if -1 == reply.FileId {
		// refused to give a task
		// notify the caller
		if reply.AllDone {
			worker.logPrintf("no more map tasks, switch to reduce mode\n")
			return nil
		} else {
			// worker.logPrintf("there is no task available for now. there will be more just a moment...\n")
			return &reply
		}
	}
	worker.logPrintf("got map task on file %v %v\n", reply.FileId, reply.FileName)

	// given a task
	return &reply
}

func (worker *AWorker) writeToFiles(fileId int, nReduce int, intermediate []KeyValue) {
	kvas := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, nReduce)
		kvas[index] = append(kvas[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		// ioutils deprecated
		// use this as equivalent
		tempfile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		outname := fmt.Sprintf("mr-%v-%v", fileId, i)
		err = os.Rename(tempfile.Name(), outname)
		if err != nil {
			worker.logPrintf("rename tempfile failed for $v\n", outname)
		}
	}
}

func (worker *AWorker) joinMapTask(fileId int) {
	args := MapTaskJoinArgs{}
	args.FileId = fileId
	args.WorkerId = worker.workerId

	reply := MapTaskJoinReply{}
	call("Coordinator.JoinMapTask", &args, &reply)

	if reply.Accept {
		worker.logPrintf("accepted\n")
	} else {
		worker.logPrintf("not accepted\n")
	}
}

func (worker *AWorker) executeMap(reply *MapTaskReply) {
	intermediate := makeIntermediateFromFile(reply.FileName, worker.mapf)
	worker.logPrintf("writing map results to file\n")
	worker.writeToFiles(reply.FileId, reply.NReduce, intermediate)
	worker.joinMapTask(reply.FileId)
}

func (worker *AWorker) askReduceTask() *ReduceTaskReply {
	args := ReduceTaskArgs{}
	args.WorkerId = worker.workerId
	reply := ReduceTaskReply{}

	worker.logPrintf("requesting for reduce task...\n")
	call("Coordinator.GiveReduceTask", &args, &reply)

	if -1 == reply.RIndex {
		// refused to give a task
		// notify the caller
		if reply.AllDone {
			worker.logPrintf("no more reduce tasks, try to terminate worker\n")
			return nil
		} else {
			// worker.logPrintf("there is no task available for now. there will be more just a moment...\n")
			return &reply
		}
	}
	worker.logPrintf("got reduce task on %vth cluster", reply.RIndex)

	// given a task
	return &reply
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceKVSlice(intermediate []KeyValue, reducef func(string, []string) string, ofile io.Writer) {
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && (intermediate)[j].Key == (intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (intermediate)[k].Value)
		}
		output := reducef((intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func readIntermediates(fileId int, reduceId int) []KeyValue {
	filename := fmt.Sprintf("mr-%v-%v", fileId, reduceId)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: %v\n", filename)
	}
	dec := json.NewDecoder(file)
	kva := make([]KeyValue, 0)
	for {
		var kv KeyValue
		err = dec.Decode(&kv)
		if err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	/*
		sort.Sort(ByKey(kva))
		for i := 0; i < len(kva); {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			// fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			// do not commit to file directly yet
			// reduce in memory then commit to file together
			*intermediate = append(*intermediate, KeyValue{
				Key: kva[i].Key,
				Value: output,
			})

			i = j
		}
	*/
	// reduceKVSlice(&kva, reducef)
	// *intermediate = append(*intermediate, kva...)
	return kva
}

func (worker *AWorker) joinReduceTask(rindex int) {
	args := ReduceTaskJoinArgs{}
	args.RIndex = rindex
	args.WorkerId = worker.workerId

	reply := ReduceTaskJoinReply{}
	call("Coordinator.JoinReduceTask", &args, &reply)

	if reply.Accept {
		worker.logPrintf("accepted\n")
	} else {
		worker.logPrintf("not accepted\n")
	}
}

func (worker *AWorker) executeReduce(reply *ReduceTaskReply) {
	outname := fmt.Sprintf("mr-out-%v", reply.RIndex)
	// ofile, err := os.Open(outname)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.FileCount; i++ {
		worker.logPrintf("generating intermediates on cluster %v\n", i)
		intermediate = append(intermediate, readIntermediates(i, reply.RIndex)...)
	}
	worker.logPrintf("total intermediate count %v\n", len(intermediate))
	tempfile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		worker.logPrintf("cannot create tempfile for %v\n", outname)
	}
	reduceKVSlice(intermediate, worker.reducef, tempfile)
	tempfile.Close()
	err = os.Rename(tempfile.Name(), outname)
	if err != nil {
		worker.logPrintf("rename tempfile failed for %v\n", outname)
	}
	worker.joinReduceTask(reply.RIndex)
}

func (worker *AWorker) process() {
	if worker.allDone {
		// must exit
		// checked by caller
	}
	if !worker.mapOrReduce {
		// process map task
		reply := worker.askMapTask()
		if reply == nil {
			// must switch to reduce mode
			worker.mapOrReduce = true
		} else {
			if reply.FileId == -1 {
				// no available tasks for now
			} else {
				// must execute
				worker.executeMap(reply)
			}
		}
	}
	if worker.mapOrReduce {
		// process reduce task
		reply := worker.askReduceTask()
		if reply == nil {
			// all done
			// must exit
			worker.allDone = true
		} else {
			if reply.RIndex == -1 {
				// noavailable tasks for now
			} else {
				// must execute
				worker.executeReduce(reply)
			}
		}
	}
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
	args.X = 99

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
