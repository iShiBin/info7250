# Requirements

Count the number of times each type of error occurred in the error.log file (attached) using Hadoop custom Counters.

# Analysis

After a manual screen of the input data file, the error type includes but not least:

* File does not exist...
* script '...' not found or unable to stat
* client sent HTTP/1.1 request without hostname
* Invalid method in request...

It might include some undiscovered type so I will categorize them to these error types reflecting the above types:

* FILE
* SCRIPT
* HTTP
* METHOD
* UNKNOWN

Note: The last one is just for precaution since manual screening may miss some error types.

In addition, all these errors have the same format like below:

```
[Sat Oct 15 11:49:11 2011][error] [client 127.0.0.1] File does not exist:...
[Fri May 03 20:10:59 2013] [error] [client 54.244.199.214] script 'C:/www/links.php' not found or unable to stat...
[Sun Mar 31 03:05:31 2013] [error] [client 91.121.29.213] client sent HTTP/1.1 request without hostname...
[Wed Jun 20 23:24:55 2012] [error] [client 78.172.244.138] Invalid method in request...
```



- It has "[error]" and [client xxx.xxx.xxx.xxx] in the text.
- It has the keyword for each error type { "FILE", "SCRIPT", "HTTP", "METHOD" };

# MapReduce Source Code

```java
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ErrorTypeCounterMR extends Configured implements Tool {

    public static class ErrorTypeCounterMRMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
        public static final String ERROR_TYPE_GROUP = "Error Counters";
        public static final String UNKNOWN_ERROR = "Unknown";
        private Set<String> errorType = new HashSet<>(Arrays.asList("File", "script", "HTTP", "method")); // case matters

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String message = value.toString();

            if (isError(message)) {
                boolean unknown = true;
                
                for (String error : errorType) {
                    if (message.contains(error)) {
                        context.getCounter(ERROR_TYPE_GROUP, error).increment(1);
                    }
                    unknown = false;
//                    break; // bug!
                }

                if (unknown) {
                    context.getCounter(ERROR_TYPE_GROUP, UNKNOWN_ERROR).increment(1);
                }
            }
        }

        public static boolean isError(String message) {
            if (message == null || message.isEmpty())
                return false;

            if (message.contains("[error]") && (message.contains("[client"))) {
                return true;
            }
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ErrorTypeCounterMR(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count # of Errors in a Type");
        job.setJarByClass(ErrorTypeCounterMR.class);
        job.setMapperClass(ErrorTypeCounterMRMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        boolean success = job.waitForCompletion(true);

        FileSystem.get(conf).delete(outputDir, true);
        return success ? 0 : 1;
    }
}
```

# MapReduce Job

Here is the running:

```bash
$ hadoop jar ErrorTypeCounterMR.jar ErrorTypeCounterMR error-log/input error-log/error-types
2018-04-08 12:33:36,292 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
2018-04-08 12:33:36,671 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2018-04-08 12:33:36,685 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ubuntu/.staging/job_1523057534388_0024
2018-04-08 12:33:36,879 INFO input.FileInputFormat: Total input files to process : 1
2018-04-08 12:33:36,927 INFO mapreduce.JobSubmitter: number of splits:1
2018-04-08 12:33:36,960 INFO Configuration.deprecation: yarn.resourcemanager.system-metrics-publisher.enabled is deprecated. Instead, use yarn.system-metrics-publisher.enabled
2018-04-08 12:33:37,066 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1523057534388_0024
2018-04-08 12:33:37,067 INFO mapreduce.JobSubmitter: Executing with tokens: []
2018-04-08 12:33:37,233 INFO conf.Configuration: resource-types.xml not found
2018-04-08 12:33:37,234 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2018-04-08 12:33:37,291 INFO impl.YarnClientImpl: Submitted application application_1523057534388_0024
2018-04-08 12:33:37,325 INFO mapreduce.Job: The url to track the job: http://ip-172-31-25-109.us-west-2.compute.internal:8088/proxy/application_1523057534388_0024/
2018-04-08 12:33:37,326 INFO mapreduce.Job: Running job: job_1523057534388_0024
2018-04-08 12:33:44,419 INFO mapreduce.Job: Job job_1523057534388_0024 running in uber mode : false
2018-04-08 12:33:44,420 INFO mapreduce.Job:  map 0% reduce 0%
2018-04-08 12:33:48,467 INFO mapreduce.Job:  map 100% reduce 0%
2018-04-08 12:33:53,496 INFO mapreduce.Job:  map 100% reduce 100%
2018-04-08 12:33:53,504 INFO mapreduce.Job: Job job_1523057534388_0024 completed successfully
2018-04-08 12:33:53,590 INFO mapreduce.Job: Counters: 57
    File System Counters
        FILE: Number of bytes read=6
        FILE: Number of bytes written=408857
        FILE: Number of read operations=0
        FILE: Number of large read operations=0
        FILE: Number of write operations=0
        HDFS: Number of bytes read=2009160
        HDFS: Number of bytes written=0
        HDFS: Number of read operations=8
        HDFS: Number of large read operations=0
        HDFS: Number of write operations=2
    Job Counters 
        Launched map tasks=1
        Launched reduce tasks=1
        Data-local map tasks=1
        Total time spent by all maps in occupied slots (ms)=5074
        Total time spent by all reduces in occupied slots (ms)=4664
        Total time spent by all map tasks (ms)=2537
        Total time spent by all reduce tasks (ms)=2332
        Total vcore-milliseconds taken by all map tasks=2537
        Total vcore-milliseconds taken by all reduce tasks=2332
        Total megabyte-milliseconds taken by all map tasks=5195776
        Total megabyte-milliseconds taken by all reduce tasks=4775936
    Map-Reduce Framework
        Map input records=19980
        Map output records=0
        Map output bytes=0
        Map output materialized bytes=6
        Input split bytes=124
        Combine input records=0
        Combine output records=0
        Reduce input groups=0
        Reduce shuffle bytes=6
        Reduce input records=0
        Reduce output records=0
        Spilled Records=0
        Shuffled Maps =1
        Failed Shuffles=0
        Merged Map outputs=1
        GC time elapsed (ms)=158
        CPU time spent (ms)=990
        Physical memory (bytes) snapshot=544432128
        Virtual memory (bytes) snapshot=6201020416
        Total committed heap usage (bytes)=354942976
        Peak Map Physical memory (bytes)=328085504
        Peak Map Virtual memory (bytes)=3096252416
        Peak Reduce Physical memory (bytes)=216346624
        Peak Reduce Virtual memory (bytes)=3104768000
    Error Counters
        File=19372
        HTTP=197
        method=2
        script=139
    Shuffle Errors
        BAD_ID=0
        CONNECTION=0
        IO_ERROR=0
        WRONG_LENGTH=0
        WRONG_MAP=0
        WRONG_REDUCE=0
    File Input Format Counters 
        Bytes Read=2009036
    File Output Format Counters 
        Bytes Written=0
```

# Observation

Here are the counts of each error type according to the above console output (Error Counters), and there is no unknown error type.

> File=19372
HTTP=197
method=2
script=139
