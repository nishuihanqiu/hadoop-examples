# hadoop 命令
---
### Usage − hadoop [--config confdir] COMMAND
- namenode -format 
    > Formats the DFS filesystem.
- secondarynamenode
    > Runs the DFS secondary namenode.
- namenode 
    > Runs the DFS namenode.
- datanode 
    > Runs a DFS datanode.
- dfsadmin 
    > Runs a DFS admin client.
- mradmin 
    > Runs a Map-Reduce admin client.
- fsck 
    > Runs a DFS filesystem checking utility.
- fs 
    > Runs a generic filesystem user client.
- balancer 
    > Runs a cluster balancing utility.
- oiv 
    > Applies the offline fsimage viewer to an fsimage.
- fetchdt 
    > Fetches a delegation token from the NameNode.
- jobtracker 
    > Runs the MapReduce job Tracker node.
- pipes 
    > Runs a Pipes job.
- tasktracker 
    > Runs a MapReduce task Tracker node.
- historyserver 
    > Runs job history servers as a standalone daemon.
- job 
    > Manipulates the MapReduce jobs.
- queue 
    > Gets information regarding JobQueues.
- version 
    > Prints the version.
- jar<jar> 
    > Runs a jar file.
- distcp <srcurl> <desturl>   
    > Copies file or directories recursively.
- distcp2 <srcurl> <desturl>  
    > DistCp version 2.
- archive -archiveName NAME -p <parent path> <src>* <dest>  
    > Creates a hadoop archive.
- classpath  
    > Prints the class path needed to get the Hadoop jar and the required libraries.
- daemonlog 
    > Get/Set the log level for each daemon
               

### Usage − hadoop job [GENERIC_OPTIONS]
* -submit <job-file>
    > Submits the job.
	
* -status <job-id>
    > Prints the map and reduce completion percentage and all job counters.

* -counter <job-id> <group-name> <countername>
    > Prints the counter value.

* -kill <job-id>
    > Kills the job.

* -events <job-id> <fromevent-#> <#-of-events>
    > Prints the events' details received by jobtracker for the given range.

* -history [all] <jobOutputDir> - history < jobOutputDir>
    > Prints job details, failed and killed tip details. More details about the job such as successful tasks and task attempts made for each task can be viewed by specifying the [all] option.

* -list[all]
    > Displays all jobs. -list displays only jobs which are yet to complete.

* -kill-task <task-id>
    > Kills the task. Killed tasks are NOT counted against failed attempts.

* -fail-task <task-id>
    > Fails the task. Failed tasks are counted against failed attempts.

* -set-priority <job-id> <priority>
    > Changes the priority of the job. Allowed priority values are VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW