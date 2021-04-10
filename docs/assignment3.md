# Assignment 3. Sparkling Spark

## Set up

For this assignment I created the Docker machine to run Spark using the following command: 
```
docker create --name hey-spark -it -p 8080:8080 -p 9001:9001 -p 4040:4040 rubigdata/course:a3
```

I use Docker Desktop to run the Docker container after I created it.

For Parts A and B of the assignment I added _zpln_ files from the assignment GitHub to Zeppelin UI and started working on the assignment in the notebooks imported.

# Assignment Part A

Part A is a part of the assignment to get to know basics of Spark. We start with the following Spark code: 
 
 ```
 %spark
val rdd = sc.parallelize(0 to 999,8)
 ```

### ● Why did Spark fire off eight different tasks? 

From Spark [documentation](https://spark.apache.org/docs/2.1.1/programming-guide.html#parallelized-collections) about parallelized collection I got to know that when we specify the number of partitions then Spark runs separate task on each partition. This means that specifying 8 partitions in the code above Spark executes 8 different tasks.

### ● Find longest line in 100.txt 

For this task we use Shakespeare texts. I assume that the code I wrote actually finds the longest line comparing lines lengths and returning back the longest. 
```
%spark
println( "Lines:\t" + lines.count + "\n" + 
         "Longest:\t" + lines.map(s => s.length).
                           reduce((v_i, v_j) => v_i max v_j))
```
### ● Why we used flatmap instead of map in following example? 
```
%spark
val words = lines.flatMap(line => line.split(" "))
              .filter(_ != "")
              .map(word => (word,1))
```
_map()_ function takes one element as input, processes it according to custom code and returns one element at a time. While _flatMap()_ function takes one element as input, processes it according to custom code and returns zero or more elements at a time. In our case we want to return all words in a line: line is an input and >= 0 words are the output of our _flatMap()_ function. 

### ● Explain why there are multiple result files. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/lsSpark.png "ls Hadoop")

I received this result of _ls_ command and it can be seen, that there is 2 files with data. Spark divides data in the partitions and we have not specified the number of partitions when processing word count, so data was divided into two partitions by Spark and saved as _part-00000_ and _part_00001_. 

### ● Why are the counts different?

When we started with the word counts we executed the following code: 
```
%spark
val words = lines.flatMap(line => line.split(" "))
              .filter(_ != "")
              .map(word => (word,1))
val wc = words.reduceByKey(_ + _)
wc.filter(_._1 == "Macbeth").collect
```
and as a result we get, that _Macbeth_ appeared in the text 30 times. However, the count of Macbeth is heigher after executing the following code: 
```
%spark
val words = lines.flatMap(line => line.split(" "))
              .map(w => w.toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
              .filter(_ != "")
              .map(w => (w,1))
              .reduceByKey( _ + _ )
```
, because in this code we processed the words before mapping them. We changed all the words to lower case and we replaced all the non-alphabetic symbols with empty string. When we used comparison in the next step and selected all the words containing "macbeth", then we counted all the possible combinations of Macbeth: "Macbeth's", "Macbeth,", "Macbeth.", etc.

# Assignment Part B

### ● Partitions

I used _cat /proc/cpuinfo_ command to get the number of cores in the machine that runs the docker engine, and I got the result of 16 processors used. Below is the description of one of them: the only place differed was _apicid_ and processor number, all the other parameters are the same, because only my laptop processor used.

```
processor	: 15
vendor_id	: AuthenticAMD
cpu family	: 23
model		: 96
model name	: AMD Ryzen 7 4800H with Radeon Graphics
stepping	: 1
microcode	: 0xffffffff
cpu MHz		: 2894.570
cache size	: 512 KB
physical id	: 0
siblings	: 16
core id		: 7
cpu cores	: 8
apicid		: 15
initial apicid	: 15
fpu		: yes
fpu_exception	: yes
cpuid level	: 13
wp		: yes
flags		: <were removed by me, too many>
bugs		: <were removed by me>
bogomips	: 5790.03
TLB size	: 3072 4K pages
clflush size	: 64
cache_alignment	: 64
address sizes	: 48 bits physical, 48 bits virtual
```
Partitioner is an object that defines how the elements in a key-value pair RDD are partitioned by key. Maps each key to a partition ID, from 0 to numPartitions - 1. Initially we don't have a partitioner and we need to specify it manually: 
```
%spark
val rddPairsPart2 = rddPairs.partitionBy(new HashPartitioner(2))
printf( "Number of partitions: %d\n", rddPairsPart2.partitions.length)
```
The last line of the code returns the following result: _Number of partitions: 2_. 

In the next step we group key-value pairs by keys (_x%100_) with ___val rddPairsGroup = rddPairs.groupByKey()___ command. When we print the debug string, we receive the following result (1). When we call the same code again, Spark does grouping again and organizes the data differently. 

(1)
```
res6: String =
(8) ShuffledRDD[5] at groupByKey at <console>:27 []
 +-(8) MapPartitionsRDD[3] at map at <console>:27 []
    |  ParallelCollectionRDD[2] at parallelize at <console>:28 []
```

When we executed the following code (2) before, we specified the number of partitions. And when we execute the code ___printf( "Number of partitions: %d\n", rddPairsGroup.partitions.length)___ later in the assognment after all the steps mentioned above, Spark still remembers that we specified the number of partitions and returns _Number of partitions: 8_ as a result. 

(2)
```
%spark
import org.apache.spark.HashPartitioner
val rddRange = sc.parallelize(0 to 999,8)
val rddPairs = rddRange.map(x => (x % 100, 1000 - x))
val rddPairsGroup = rddPairs.groupByKey()
```

We see that Spark remembers the HashPartitioner it used to create the grouping. Because the original data _rddPairsGroup_ had 8 partitions, it opts to create 8 partitions in the result as well. For the _rddPairsPart2_ the original data has 2 partitions, so the result will also be with 2 partitions.

When we use _map()_ and _mapValue()_ functions the situation with partitioner is different. After _map()_ function execution RDD will not remember the partitioner we assigned, because keys might been changed. In this case Spark uses default partitioning. However, _mapValue()_ changes only values and keys stay the same, so RDD keeps the partitioner in memory and it is used later in the code.


[< back](index.md)