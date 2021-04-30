# Assignment 5. Spark Structured Streaming

## Set up

For this assignment I reused the container created in [previous blog](assignment3.md).

For the assignment I added _zpln_ file from the assignment's GitHub to Zeppelin UI and started working on the tasks in the notebook imported.

## Work process

In this blog I descibe Spark Structures Streaming query processing. As an example for the analysis I use data from an online marketplace, where items from RuneScape game are sold. 

In the previous blogs we used HDFS system, which settings can affect our workflow. So before starting we restart the interpreter to get rid of possible errors. 

```
%sh
sed -i -e 's|hdfs://localhost:9000|file:///tmp|' /opt/hadoop/etc/hadoop/core-site.xml
```

We start with simulation that generated a stream of events inside our Docker container. Simulator is a Python program, that generates the RuneScape-like output to port 9999. We download the code and transfer it to our Docker container, then we execute the program.

To work with the streams we do a couple of imports: _import spark.implicits.__ and _import org.apache.spark.sql.streaming.Trigger_. After that we create a streaming dataframe socket which reads stream on localhost port 9999 as follows:

```
val socketDF = spark.readStream
  .format("socket").option("host", "0.0.0.0")
  .option("port", 9999).load()
```

At this point we start with the data processing. When I first started with this process, then my _memoryQuery_ terminated with bunch of errors. However, after re-importing the notebook everything worked again. So, in this step we want to write data to our in-memory dataframe from the TCP/IP connection we are listening on port 9999. We read all data that comes from the socket for one second (because the amount of data is big and we donät want to fill up our Docker container memory) and then stop. _memoryQuery_ is StreamingQuery that splits data into separate lines based on newline. 

```
val streamWriterMem = socketDF
  .writeStream.outputMode("append").format("memory")

val memoryQuery = streamWriterMem  
  .queryName("memoryDF").start()

memoryQuery.awaitTermination(1000)
  
memoryQuery.stop()
```

After reading and saving data from the stream, we want to see what was saved from there. For that we use SQL in out Spark. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/swordOutputs.png "output query")



[< back](index.md)