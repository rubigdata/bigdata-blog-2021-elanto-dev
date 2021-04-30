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

At this point we start with the data processing. When I first started with this process, then my _memoryQuery_ terminated with bunch of errors. However, after re-importing the notebook everything worked again. So, in this step we want to write data to our in-memory dataframe from the TCP/IP connection we are listening on port 9999. We read all data that comes from the socket for 10 seconds (because the amount of data is big and we donät want to fill up our Docker container memory) and then stop. _memoryQuery_ is StreamingQuery that splits data into separate lines based on newline. 

```
val streamWriterMem = socketDF
  .writeStream.outputMode("append").format("memory")

val memoryQuery = streamWriterMem  
  .queryName("memoryDF").start()

memoryQuery.awaitTermination(10000)
  
memoryQuery.stop()
```

After reading and saving data from the stream, we want to see what was saved from there. For that we use SQL in out Spark. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/swordOutputs.png "output query")

In the screenshot we printed the first 10 elements we got, however if we want to see how many we actually read from the stream, then we need to execute _spark.sql("select count(*) from memoryDF").show()_ command. The result in our case is 949 rows. 

### Parsing data

We read data line by line without any structure, but when we know the structure we can create objects out of this data. So we start with creating class _RuneData_, with material, type and price attributes: _RuneData(material: String, tpe: String, price: Integer)_ where _tpe_ is type. Then we use regular expressions to parse the data: 

```
val myregex = "\"^([A-Z].+) ([A-Z].+) was sold for (\\\\d+)\""
val q = f"select regexp_extract(value, $myregex%s, 1) as material, regexp_extract(value, $myregex%s, 2) as tpe, cast(regexp_extract(value, $myregex%s, 3) as Integer) as price from memoryDF"
spark.sql(q).as[RuneData].show(10, false)

Output: 
+--------+----------------+-----+
|material|tpe             |price|
+--------+----------------+-----+
|Bronze  |Spear           |1171 |
|Rune    |Scimitar        |13031|
|White   |Scimitar        |3463 |
|Rune    |Hasta           |7221 |
|White   |Spear           |3381 |
|Adamant |Dagger          |2024 |
|Black   |Halberd         |12857|
|Adamant |Dagger          |2056 |
|Mithril |Two-handed sword|8434 |
|Black   |Sword           |2180 |
+--------+----------------+-----+
```

Here we see first 10 items we saw in previous query, but all the items are now objects, not just lines of text. 

### Stream Processing

Previous examples consisted of reading data and then processing it. However, in real life it is important to process the data while reading it in continues streaming. In this part we try to process data in running stream. We use the following command to run the stream continuosly and print it to the console (no save to memory). It will continue reading in batches (10 lines batch) unless we stop it with _consoleQuery.stop()_: 

(1)
```
val consoleQuery = socketDF
  .writeStream
  .outputMode("append")
  .format("console")
  .start()
```

We use the same class created above to structure the data. When reading SQL we use the same regex string to get all the needed values for the object. 

```
val myregex = "\"^([A-Z].+) ([A-Z].+) was sold for (\\\\d+)\""
val q = f"select regexp_extract(value, $myregex%s, 1) as material, regexp_extract(value, $myregex%s, 2) as tpe, cast(regexp_extract(value, $myregex%s, 3) as Integer) as price from runeUpdatesDF"
val runes = spark.sql(q).as[RuneData]
```

When we start reading query we use _runes_ instead of _socketDF_, but the other parts of command are exactly like in (1). When we want to stop reading data, we just use _consoleQuery.stop()_ command. 

### Save stream data

In real cluster we don't write data to console, but we want to save it from the stream. To save data we firstly create the folder _/opt/hadoop/share/runedata_ where our data will be saved. Then we specify that now the data read from the stream will be saved to the file and start the writing process: 

```
val streamWriterDisk = runes
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", "file:///tmp/checkpoint")
  .trigger(Trigger.ProcessingTime("2 seconds"))

val stream2disk = streamWriterDisk
  .start("file:///opt/hadoop/share/runedata")
```

To check if the stream is active I used the following command. We can see, that there is active stream. This command can be used with all the _stream.start()_ commands we used before. 

```
spark.streams.active

Output: 
res23: Array[org.apache.spark.sql.streaming.StreamingQuery] = Array(org.apache.spark.sql.execution.streaming.StreamingQueryWrapper@3cc0d3b1)
```

The amount of memory used is slowly increasing while we run the program. To check how much space was used we can execute the following commad: _du --si /opt/hadoop/share/runedata_. 

### Working with data collected

After we collected the data we want to analyze it. We start with the following command: 

```
val runes = spark
  .read
  .parquet("file:///opt/hadoop/share/runedata/part-*")
  .createOrReplaceTempView("runes")
```

We want to know the average price of the items made from the materials. For this we need to group all the items by the materials and then get their average price. I sorted the results in descending order: 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/itemsByMaterial.png "items and price by material")

The following graph shows min and max prices for items made of different materials. Zeppelin is really good in creating very good visual diagrams out of data. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/itemsByMaterialMin.png "items and price by material")

The following example shows how many items total were sold while we were reading the stream. The second part returned the number of Rune items that were sold. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/runeItems.png "rune items sold")

The following example shows how many items of each type were sold. The types were sorted by ascending order. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/itemsByType.png "rune items sold")

The following example shows how many gold was spent on buying swords. I considered all type of swords, so we searched for all the types containing _sword_ as substring. 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/swords.png "swords")

### Result

In this blog we learned how to work with streams and analyse data using SQL. In my opinion it is the most interesting assignment during the course. 

[< back](index.md)