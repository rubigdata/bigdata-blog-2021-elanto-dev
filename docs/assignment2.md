# Assignment 2. Hello Hadoop

## [Setup description](hadoop-setup.md)

## Own Map Reduce job

I describe the HDFS setup in the Setup description part of this blog. Answer to the first question mentioned in the assignment is at the end of **Setup** post. Here I will focus on my own Map Reduce implementation. I am starting with downloading the Shakespeare text from GitHub and using it as an input for distributed file system I created in previous step. The commands I used:
```
cd /opt/hadoop
wget https://raw.githubusercontent.com/rubigdata-dockerhub/hadoop-dockerfile/master/100.txt
hdfs dfs -put 100.txt input
```
The following step is to add and modify Java code, used for Map Reduce functionality. Initially I needed to create the class and I used command _nano WordCount.java_ to create file and open an editor. Then I copied the code from [the relevant Hadoop documentation](https://hadoop.apache.org/docs/r3.2.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code). 

One of the tasks was to create the code that counts how many time Romeo and Juliet were mentioned. To do so I changed the _map_ function the following way: 

![alt text](/images/javacode.png "Title")

If _.find()_ function found the word from the brackets, then it added onde to the context with "Romeo(Juliet) count: " key. In _map_ function I also counted the words for the statistics. If it is needed to find any other word, line, character, then _map_ function should be modified for that process. 

After _WordCount.java_ file was modified, I am ready to compile and run it. The following screenshot shows how I deleleted previously created output directory, compiled and ran the code (_substitution_ errors were ignored): 

![alt text](/images/hdfscommands.png "Title")
![alt text](/images/mapreduceoutput.png "Title")

After the code was ran, I had a look at the output directory, to get to know if Romeo appeared more often than Juliet. The results were the following and showed, that Romeo indeed appeared exactly 2 times more often thab Juliet: 

![alt text](/images/outputfolder.png "Title")

In this assignment I learned how to use HDFS system and how to work with Map Reduce tools. 


[< back](index.md)