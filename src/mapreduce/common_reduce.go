package mapreduce

import (
	// "hash/fnv"
	"fmt"
	"os"
	// "io/ioutil"
	"encoding/json"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	key_values := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fn := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fn)
		if err != nil {
			fmt.Printf("Error reading %s", fn)
		} else {
			enc := json.NewDecoder(file)
			for {
				var kv KeyValue
				err := enc.Decode(&kv)
				if err != nil {break}
				_, ok := key_values[kv.Key]
				if !ok {key_values[kv.Key] = make([]string, 0)}
				key_values[kv.Key] = append(key_values[kv.Key], kv.Value)
			}
		}
		file.Close()
	}
	r_wc := make(map[string]string)
	for k, v := range key_values {
		r_wc[k] = reduceF(k, v)
	}
	var ss []KeyValue
	for k, v := range r_wc {
		ss = append(ss, KeyValue{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value < ss[j].Value
	})
	file, _ := os.Create(mergeName(jobName, reduceTask))
	enc := json.NewEncoder(file)
	for _,kv := range ss {
		enc.Encode(kv)
	}
	file.Close()
}
