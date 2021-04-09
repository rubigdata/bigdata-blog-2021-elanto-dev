# Assignment 3. Sparkling Spark

## Set up

For this assignment I created the Docker machine to run Spark using the following command: 
```
docker create --name hey-spark -it -p 8080:8080 -p 9001:9001 -p 4040:4040 rubigdata/course:a3
```
When I run the Docker container after I created it I use Docker Desktop.

# Assignment Part A

For Part A of the assignment I added _zpln_ file to Zeppelin UI and started working on the assignment in the notebook imported.

### ● Why did Spark fire off eight different tasks? 

From Spark [documentation](https://spark.apache.org/docs/2.1.1/programming-guide.html#parallelized-collections) about parallelized collection I got to know that when we specify the number of partitions then Spark runs separate task on each partition. This means that specifying 8 partitions in 
```
%spark
val rdd = sc.parallelize(0 to 999,8)
``` 
Spark executes 8 different tasks.

### ● Why would Spark create two jobs to take the sample?

Maybe answer here

### ● Find longest line in 100.txt 

I assume that the code I wrote actually finds the longest line comparing their lengths and returning back the longest. 
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

I received this result of _ls_ command and it can be seen, that there is 2 files with data. Spark divides data in the partitions and we have not specified the number of partitions when processing word count, so data was divided into two partitions and saved as _part-00000_ and _part_00001_. 

### ● Why are the counts different (and, why are they higher than before, and not lower)?

The count of Macbeth is heigher after executing the following code: 
```
%spark
val words = lines.flatMap(line => line.split(" "))
              .map(w => w.toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
              .filter(_ != "")
              .map(w => (w,1))
              .reduceByKey( _ + _ )
```
, because we processed the words before mapping them. We changed all the words to lower case and we replaced all the non-alphabetic symbols with empty string. When we used comparison in the next step and selected all the words containing "macbeth", then we counted all the possible combinations of Macbeth: "Macbeth's", "Macbeth,", "Macbeth.", etc.

# Assignment Part B



[< back](index.md)