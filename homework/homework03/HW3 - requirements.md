Due by 11:59pm on Sunday,Feb.18

**PART 1 – Reading Assignment (Attached)**

   - Big Data Processing with Hadoop-MapReduce in Cloud Systems.

   - Mapreduce is good enough? 

**PART 2 – Programming Assignment**

All hadoop commands are invoked by the bin/hadoop script. Running the hadoop script without any arguments prints the description for all commands.
Usage: hadoop [--config confdir] [--loglevel loglevel] [COMMAND] [GENERIC_OPTIONS] [COMMAND_OPTIONS]
Execute each hadoop command once, and place the screenshots into a word file. If a command cannot be executed for any reason (such as, a distributed environment is needed), you may write the definition of the command, and skip execution.

<http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html>

**PART 3 – Programming Assignment**

Copy the attached ‘access.log’ file into HDFS under /logs directory.

Using the access.log file stored in HDFS, implement MapReduce to find the number of times each IP accessed the website.

**PART 4 – Programming Assignment**

Download the following dataset and Copy all the files to a folder in HDFS

MovieLens 1M - Stable benchmark dataset. 1 million ratings from 6000 users on 4000 movies.

<https://grouplens.org/datasets/movielens/1m/>

Write a MapReduce to find the number of males and females in the movielens dataset 

**PART 5 – Programming Assignment**

Write a MapReduce to find the number of movies rated by different users in the MovieLens dataset.