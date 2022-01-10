# Lab 1 MapReduce

offical website https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

## How it works

In general, one coordinator distributes tasks, while many workers ask for tasks and execute tasks. The coordinator first distributes map tasks, then distributes reduce tasks, based on the results of previous map tasks. 

Each of the map tasks do the following things:
- seperate a designated file into a word array, with each word associated a count of 1.
- let each word with its count go to a cluster, based on its hash value.
- the ith file would have `nReduce` intermediate files, named `mr-i-j`, where the j is the designated address derived from the hash value of a word.

Each of the reduce tasks do the following things:
- fix j, gather all mr-i-j files for all i, reading all of the words and counts.
- for each j, aggregate the words by sorting the count the words.
- submit the result to a file `mr-out-j`.

Because j is computed based on hash values, same words would certainly go to the same file. Reducing on different j would produce files that can be directly concatenated together without further aggregation or counting.

## Task Management

The coordinator manages all tasks. At initialization, the coordinator initializes a number of map tasks based on the number of files. Then workers are initialized and distributed tasks. 

When a task is done, the worker joins the task with the coordinator. When all map tasks are joined, the coordinator initializes a series of reduce tasks, based on `nReduce`. Then workers are distributed reduce tasks.

When all reduce tasks are done, the workers stop polling for tasks and exits. The caller respond `true` to rpc checking whether it is done.
