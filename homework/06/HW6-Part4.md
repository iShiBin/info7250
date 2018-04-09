# Requirements

Find the Top 10 Largest TotalDue Amount** (from the SalesOrder.csv file) using the Top 10 algorithms as explained in the lectures and demonstrated in the Lecture 08 slides.

# Analysis

- the interesting field is at column[22] - TotalDue
- use PriorityQueue instead of TreeMap to reduce the memory usage
- use input parameter to make this program more general for top 5, top 100 etc.

**Notes**: The output of top 10 `TotalDue` is distinct, meaning the same value will only show once in the final top ranking list.

# Data Cleaning

Remove the header of the input data using command sed -i '1d' SalesOrder.csv, and then put it in the HDFS by `fs -put SalesOrder.csv sales-order/input`

# Souce Code

```java
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopTotalDueMR extends Configured implements Tool {
    
    private static int k = 10;//dynamic k: hadoop jar topk.jar TopTotalDueMR -D topk=10 /input_dir /output_dir

    public static class RankMapper extends Mapper<Object, Text, NullWritable, DoubleWritable> {
        
//      PriorityQueue is better than TreeMap since TotalDue is the only value needed
        private PriorityQueue<Double> ranking = new PriorityQueue<>(k); // init size to k
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("topk", k); // default top 10
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            Double due = new Double(tokens[22]);
                        
            if(ranking.size() < k) {
                if (!ranking.contains(due)) ranking.add(due);
            } else if (ranking.peek() < due) {
                if (!ranking.contains(due)) { // same value won't be added twice
                    ranking.remove();
                    ranking.add(due);
                }
            }
        }
        
        @Override
        protected void cleanup (Context context) throws IOException, InterruptedException {
            for(Double due: ranking) {
                context.write(NullWritable.get(), new DoubleWritable(due));
            }
        }
    }

    public static class RankReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {

        private PriorityQueue<Double> ranking = new PriorityQueue<>(k); // init size to k

        @Override
        public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            
            for (DoubleWritable v: values) {
                double due = v.get(); 
                
                if(ranking.size() < k) {
                    if (!ranking.contains(due)) ranking.add(due);
                } else if (ranking.peek() < due) {
                    if (!ranking.contains(due)) { // same value won't be added twice
                        ranking.remove();
                        ranking.add(due);
                    }
                }
            }
            
            for(Double due: ranking) {
                context.write(NullWritable.get(), new DoubleWritable(due));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Top K Board");
        job.setJarByClass(TopTotalDueMR.class);

        job.setMapperClass(RankMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        System.out.println("Get the top " + k +". You can set the k value: hadoop jar topk.jar TopTotalDueMR -D topk=10 /input_dir /output_dir");
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new TopTotalDueMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}

```

# Execution

```bash
$ hadoop jar TopTotalDueMR.jar TopTotalDueMR sales-order/input sales-order/top10-pattern
Get the top 10. You can set the k value: hadoop jar topk.jar TopTotalDueMR -D topk=10 /input_dir /output_dir
2018-04-08 16:34:14,477 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
2018-04-08 16:34:15,166 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ubuntu/.staging/job_1523057534388_0029
2018-04-08 16:34:15,365 INFO input.FileInputFormat: Total input files to process : 1
2018-04-08 16:34:15,414 INFO mapreduce.JobSubmitter: number of splits:1
2018-04-08 16:34:15,450 INFO Configuration.deprecation: yarn.resourcemanager.system-metrics-publisher.enabled is deprecated. Instead, use yarn.system-metrics-publisher.enabled
2018-04-08 16:34:15,575 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1523057534388_0029
2018-04-08 16:34:15,576 INFO mapreduce.JobSubmitter: Executing with tokens: []
2018-04-08 16:34:15,744 INFO conf.Configuration: resource-types.xml not found
2018-04-08 16:34:15,744 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2018-04-08 16:34:15,796 INFO impl.YarnClientImpl: Submitted application application_1523057534388_0029
2018-04-08 16:34:15,831 INFO mapreduce.Job: The url to track the job: http://ip-172-31-25-109.us-west-2.compute.internal:8088/proxy/application_1523057534388_0029/
2018-04-08 16:34:15,832 INFO mapreduce.Job: Running job: job_1523057534388_0029
2018-04-08 16:34:21,916 INFO mapreduce.Job: Job job_1523057534388_0029 running in uber mode : false
2018-04-08 16:34:21,917 INFO mapreduce.Job:  map 0% reduce 0%
2018-04-08 16:34:25,962 INFO mapreduce.Job:  map 100% reduce 0%
2018-04-08 16:34:31,010 INFO mapreduce.Job:  map 100% reduce 100%
2018-04-08 16:34:31,017 INFO mapreduce.Job: Job job_1523057534388_0029 completed successfully
2018-04-08 16:34:31,117 INFO mapreduce.Job: Counters: 53
    File System Counters
        FILE: Number of bytes read=106
        FILE: Number of bytes written=410477
        FILE: Number of read operations=0
        FILE: Number of large read operations=0
        FILE: Number of write operations=0
        HDFS: Number of bytes read=5299495
        HDFS: Number of bytes written=118
        HDFS: Number of read operations=8
        HDFS: Number of large read operations=0
        HDFS: Number of write operations=2
    Job Counters 
        Launched map tasks=1
        Launched reduce tasks=1
        Data-local map tasks=1
        Total time spent by all maps in occupied slots (ms)=5168
        Total time spent by all reduces in occupied slots (ms)=5048
        Total time spent by all map tasks (ms)=2584
        Total time spent by all reduce tasks (ms)=2524
        Total vcore-milliseconds taken by all map tasks=2584
        Total vcore-milliseconds taken by all reduce tasks=2524
        Total megabyte-milliseconds taken by all map tasks=5292032
        Total megabyte-milliseconds taken by all reduce tasks=5169152
    Map-Reduce Framework
        Map input records=31465
        Map output records=10
        Map output bytes=80
        Map output materialized bytes=106
        Input split bytes=131
        Combine input records=0
        Combine output records=0
        Reduce input groups=1
        Reduce shuffle bytes=106
        Reduce input records=10
        Reduce output records=10
        Spilled Records=20
        Shuffled Maps =1
        Failed Shuffles=0
        Merged Map outputs=1
        GC time elapsed (ms)=149
        CPU time spent (ms)=1390
        Physical memory (bytes) snapshot=576819200
        Virtual memory (bytes) snapshot=6205841408
        Total committed heap usage (bytes)=382205952
        Peak Map Physical memory (bytes)=347410432
        Peak Map Virtual memory (bytes)=3097964544
        Peak Reduce Physical memory (bytes)=229408768
        Peak Reduce Virtual memory (bytes)=3107876864
    Shuffle Errors
        BAD_ID=0
        CONNECTION=0
        IO_ERROR=0
        WRONG_LENGTH=0
        WRONG_MAP=0
        WRONG_REDUCE=0
    File Input Format Counters 
        Bytes Read=5299364
    File Output Format Counters 
        Bytes Written=118
```

# Result

>140042.1209
142312.2199
158056.5449
145741.8553
145454.366
182018.6272
166537.0808
187487.825
165028.7482
170512.6689

Note: The result is not in order as reduce won't order the output.