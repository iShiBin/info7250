### Copy the attached ‘access.log’ file into HDFS under /logs directory

- `hadoop fs -put access.log /user/ubuntu/ip/input` to put it to the HDFS

### Implement MapReduce to find the number of times each IP accessed the website

#### Step 1: Create a Mapper class

```java
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().split(" ")[0]);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
```

#### Step 2: Create a Reducer class

```java
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
```

#### Step 4: Compile the whole project to a jar package

```bash
$hadoop com.sun.tools.javac.Main IPCount.java
$jar cf ip.jar IPCount*.class
```

#### Step 5: Run the hadoop jar command

```bash
$hadoop jar ip.jar IPCount /user/ubuntu/ip/input /user/ubuntu/ip/output
```

And here is the console output.

```
 018-02-16 11:23:44,364 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
 018-02-16 11:23:44,776 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
 018-02-16 11:23:44,789 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ubuntu/.staging/job_1518808686544_0001
 018-02-16 11:23:44,997 INFO input.FileInputFormat: Total input files to process : 1
 018-02-16 11:23:45,469 INFO mapreduce.JobSubmitter: number of splits:1
 018-02-16 11:23:45,515 INFO Configuration.deprecation: yarn.resourcemanager.system-metrics-publisher.enabled is deprecated. Instead, use yarn.system-metrics-publisher.enabled
 018-02-16 11:23:45,686 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1518808686544_0001
 018-02-16 11:23:45,687 INFO mapreduce.JobSubmitter: Executing with tokens: []
 018-02-16 11:23:45,910 INFO conf.Configuration: resource-types.xml not found
 018-02-16 11:23:45,910 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
 018-02-16 11:23:46,365 INFO impl.YarnClientImpl: Submitted application application_1518808686544_0001
 018-02-16 11:23:46,409 INFO mapreduce.Job: The url to track the job: http://ip-172-31-25-109.us-west-2.compute.internal:8088/proxy/application_1518808686544_0001/
 018-02-16 11:23:46,409 INFO mapreduce.Job: Running job: job_1518808686544_0001
 018-02-16 11:23:53,521 INFO mapreduce.Job: Job job_1518808686544_0001 running in uber mode : false
 018-02-16 11:23:53,522 INFO mapreduce.Job:  map 0% reduce 0%
 018-02-16 11:23:58,567 INFO mapreduce.Job:  map 100% reduce 0%
 018-02-16 11:24:03,600 INFO mapreduce.Job:  map 100% reduce 100%
 018-02-16 11:24:04,611 INFO mapreduce.Job: Job job_1518808686544_0001 completed successfully
 018-02-16 11:24:04,692 INFO mapreduce.Job: Counters: 53
	File System Counters
		FILE: Number of bytes read=39401
		FILE: Number of bytes written=488173
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=3497785
		HDFS: Number of bytes written=31931
		HDFS: Number of read operations=8
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=2677
		Total time spent by all reduces in occupied slots (ms)=2932
		Total time spent by all map tasks (ms)=2677
		Total time spent by all reduce tasks (ms)=2932
		Total vcore-milliseconds taken by all map tasks=2677
		Total vcore-milliseconds taken by all reduce tasks=2932
		Total megabyte-milliseconds taken by all map tasks=2741248
		Total megabyte-milliseconds taken by all reduce tasks=3002368
	Map-Reduce Framework
		Map input records=35111
		Map output records=35111
		Map output bytes=639285
		Map output materialized bytes=39401
		Input split bytes=118
		Combine input records=35111
		Combine output records=1945
		Reduce input groups=1945
		Reduce shuffle bytes=39401
		Reduce input records=1945
		Reduce output records=1945
		Spilled Records=3890
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=137
		CPU time spent (ms)=1720
		Physical memory (bytes) snapshot=556662784
		Virtual memory (bytes) snapshot=5313720320
		Total committed heap usage (bytes)=383778816
		Peak Map Physical memory (bytes)=326144000
		Peak Map Virtual memory (bytes)=2651193344
		Peak Reduce Physical memory (bytes)=230518784
		Peak Reduce Virtual memory (bytes)=2662526976
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=3497667
	File Output Format Counters 
		Bytes Written=31931
```

### Hadoop Output

Here is the first 10 lines of the output. Please refer to `IP Output.txt` for all the output.

>1.162.207.87	4
>
>1.170.44.84	83
>
>1.192.146.100	1
>
>1.202.184.142	1
>
>1.202.184.145	1
>
>1.202.89.134	2
>
>1.234.2.41	12
>
>1.56.79.5	4
>
>1.59.91.151	4
>
>1.62.189.221	4

### Verification

To verify, I choose the first IP and the last one in the sample output above, and then used `wc` to count the occurance.

```
Bin:homework03 bin$ grep 1.162.207.87 access.log | wc -l
       4
Bin:homework03 bin$ grep 1.62.189.221 access.log | wc -l
       4
```

The results matches the one using hadoop.

### Reference

The Java source code is as below.

```java
// file name: IPCount.java
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IPCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().split(" ")[0]);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "IP Count");
    job.setJarByClass(IPCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
```



