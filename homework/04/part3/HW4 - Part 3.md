# Requirement
Write one MapReduce program using each of the classes that extend FileInputFormat<k,v>
(CombineFileInputFormat, FixedLengthInputFormat, KeyValueTextInputFormat, NLineInputFormat, SequenceFileInputFormat, TextInputFormat)
http://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html
You could use any input file of your choice. The size of the input files is not important. The MR programs could simply do counting, or any other analysis you choose.

Note: The hadoop version I am in use is 3.0.0 so I refer to http://hadoop.apache.org/docs/r3.0.0/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html
# CombineFileInputFormat
The typical scenario to utilize this `CombineFileInputFormat` is to solve [The Small Files Problem](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/).

## Input
Split the whole NYSE data file to 9212 files, each of which is around 50K size, and has 1000 lines (except the last line). The command to use is `split -a 3 nyse_all_prices.csv` in linux.

Then update all these small files to HDFS using `fs -put * nyse/split/input`.
```
ubuntu@ip-172-31-25-109:~/downloads/nyse_clean$ fs -count nyse/split/input
           1         9212          511082247 nyse/split/input
```

## Map Reduce Program

```java
/** file name: FileInputFormatCombine.java
 * Demo: CombineFileInputFormat
 */

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class FileInputFormatCombine extends Configured implements Tool {

    public static class StockMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            Text symbol = new Text();
            symbol.set(tokens[1]);

            DoubleWritable price = new DoubleWritable();
            price.set(Double.parseDouble(tokens[4]));

            context.write(symbol, price);
        }
    }

    public static class StockReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable average = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            double sum = 0;
            int counter = 0;
            for(DoubleWritable price: values) {
                sum += price.get();
                counter += 1;
            }
            average.set(sum/counter);
            context.write(key, average);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Stock Average Price");
        job.setJarByClass(FileInputFormatCombine.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(CombineTextInputFormat.class); // specify the input format

        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true)?0:1;
    }

    /**
     * Implementing the Tool interface for MapReduce driver:
     * hadoop jar file.jar MainClass -D mapreduce.input.fileinputformat.split.maxsize=128 /input /output
     * @param args
     */
    public static void main (String[] args)  {

        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new FileInputFormatCombine(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
```

## Execution
Compile and pack the classes to JAR file `fifc.jar`.
`hadoop com.sun.tools.javac.Main FileInputFormatCombine.java`
`jar cf fifc.jar FileInputFormatCombine*.class`
``
This Java program implements the Tool interface for MapReduce driver, so it is easy to change the parameter `mapreduce.input.fileinputformat.split.minsize` in the command line to observe its impact.

### Use `CombineTextInputFormat`
Run this command `hadoop jar fifc.jar FileInputFormatCombine nyse/split/input nyse/split/default/output`, and it takes 46' to finish. (Refer to FileInputFormatCombine.console.output)

Here is some comparation of different `mapreduce.input.fileinputformat.split.minsize` and the runnign time.

```
| mapreduce.input.fileinputformat.split.minsize (M) | running time (seconds) |
|--------------------------------------------------:|------------------------|
|                                                64 | 48                     |
|                                               128 | 46                     |
|                                               256 | 46                     |
|                                           default | 46                     |
```

The general command is: `hadoop jar fifc.jar FileInputFormatCombine -D mapreduce.input.fileinputformat.split.minsize=64 nyse/split/input nyse/split/128/output`

### Use `TextInputFormat`

Comment this line `job.setInputFormatClass(CombineTextInputFormat.class);` in the above code so that hadoop uses the default input format (TextInputFormat.class). This map-reduce cannot even finishes in my hadoop server (AWS EC2 t2.medium with 4G RAM) becuase of 'GC overhead limit exceeded'. Here is the error message.
```
ubuntu@ip-172-31-25-109:~/documents/info7250/src$ hadoop jar fifc.jar FileInputFormatCombine nyse/split/input nyse/split/default/output
2018-02-21 12:08:00,822 INFO beanutils.FluentPropertyBeanIntrospector: Error when creating PropertyDescriptor for public final void org.apache.commons.configuration2.AbstractConfiguration.setProperty(java.lang.String,java.lang.Object)! Ignoring this property.
2018-02-21 12:08:00,893 INFO impl.MetricsConfig: loaded properties from hadoop-metrics2.properties
2018-02-21 12:08:00,944 INFO impl.MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
2018-02-21 12:08:00,944 INFO impl.MetricsSystemImpl: JobTracker metrics system started
2018-02-21 12:08:01,661 INFO input.FileInputFormat: Total input files to process : 9212
2018-02-21 12:08:01,917 INFO mapreduce.JobSubmitter: number of splits:9212
2018-02-21 12:08:02,021 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local963485496_0001
2018-02-21 12:08:02,023 INFO mapreduce.JobSubmitter: Executing with tokens: []
2018-02-21 12:08:02,126 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
2018-02-21 12:08:02,127 INFO mapreduce.Job: Running job: job_local963485496_0001
2018-02-21 12:08:02,132 INFO mapred.LocalJobRunner: OutputCommitter set in config null
2018-02-21 12:08:02,136 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 2
2018-02-21 12:08:02,136 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2018-02-21 12:08:02,136 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
2018-02-21 12:08:03,584 INFO mapreduce.Job: Job job_local963485496_0001 running in uber mode : false
2018-02-21 12:08:03,585 INFO mapreduce.Job:  map 0% reduce 0%

2018-02-21 12:09:03,748 WARN mapred.LocalJobRunner: job_local963485496_0001
java.lang.OutOfMemoryError: GC overhead limit exceeded
	at java.util.concurrent.ConcurrentHashMap.putVal(ConcurrentHashMap.java:1019)
	at java.util.concurrent.ConcurrentHashMap.putAll(ConcurrentHashMap.java:1084)
	at java.util.concurrent.ConcurrentHashMap.<init>(ConcurrentHashMap.java:852)
	at org.apache.hadoop.conf.Configuration.<init>(Configuration.java:796)
	at org.apache.hadoop.mapred.JobConf.<init>(JobConf.java:440)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.<init>(LocalJobRunner.java:245)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.getMapTaskRunnables(LocalJobRunner.java:300)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:547)
2018-02-21 12:09:04,732 INFO mapreduce.Job: Job job_local963485496_0001 failed with state FAILED due to: NA
2018-02-21 12:09:04,757 INFO mapreduce.Job: Counters: 0
```
Note: Increase the memory should fix this problem but it is not this practice's concern.

## Output
The output is the same as part 2 in this homework: the average price of stock-price-high.

# FixedLengthInputFormat

# KeyValueTextInputFormat

# NLineInputFormat

# SequenceFileInputFormat

# TextInputFormat
Please refer to `HW4 - Part 2.md`
