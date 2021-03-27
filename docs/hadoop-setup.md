Compared to the previous assignment, where I needed to re-install Windows on my machine (because Hyper-V and Docker corrupted my computer's work), this assignment went well and didn't cause me any problems.

I started with setting up HDFS filesystem in my Docker container. I created, started and attached the container, as stated in the task. I also tried some of the commands given in the tutorial to identify what machine I was running. 

Then I created the HDFS system and started it with shell executable file, wuing the following commands: 

```
hdfs namenode -format
start-dfs.sh
```

I was using the container twice and second time I didn't have to evecute this commands, because distributed file system already existed in the machine. In the next step I created home directory in distributed file system and moved there some files from the local machine. I used the following commands provided in the lecture.

```
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -mkdir input
hdfs dfs -put etc/hadoop/*.xml input
```

After this step I checked the HDFS system state using _bin/hdfs dfsadmin -report_ command and got the following result: 

![alt text](/images/report.jpg "Title")

It is seen that in created HDFS we only have one datanode, where all information is written. And it currently contains 9 blocks. Next step of the assignment will be starting _yarn_ package manager and submitting some Map Reduce jobs to _yarn_: in my case on _*.xml_ files we added to _/user/hadoop/input_ directory and $\pi$ value calculation. Results from the first Map Reduce job were moved to local machine using _hdfs dfs -get output output_ command. Concatenation of output folder _cat output/*_ returned the following result: 

![alt text](/images/catoutput.jpg "Title")

After executing Map-Resuce commands I have created the report about HDF system state and the amount of blocks in datanote increased to 19. After finishing the main part I had a look at the commands HDF system has: _hdfs dfs -help | less_. **All commands starting with _hdfs dfs_ are executed in distributed file system itself. All the commands we use after these words should start with dash, but syntax is same as in Linux system.**

[< back to Assignment 2](assignment2.md)

[< back to main](index.md)