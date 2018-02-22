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
/** file name: InputFormatCombineFile.java
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

public class InputFormatCombineFile extends Configured implements Tool {

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
        job.setJarByClass(InputFormatCombineFile.class);

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
            code = ToolRunner.run(new Configuration(), new InputFormatCombineFile(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
```

## Execution
Compile and pack the classes to JAR file `fifc.jar`.
`hadoop com.sun.tools.javac.Main InputFormatCombineFile.java`
`jar cf fifc.jar InputFormatCombineFile*.class`
``
This Java program implements the Tool interface for MapReduce driver, so it is easy to change the parameter `mapreduce.input.fileinputformat.split.minsize` in the command line to observe its impact.

### Use `CombineTextInputFormat`
Run this command `hadoop jar fifc.jar InputFormatCombineFile nyse/split/input nyse/split/default/output`, and it takes 46' to finish. (Refer to InputFormatCombineFile.console.output)

Here is some comparation of different `mapreduce.input.fileinputformat.split.minsize` and the runnign time.

```
| mapreduce.input.fileinputformat.split.minsize (M) | running time (seconds) |
|--------------------------------------------------:|------------------------|
|                                                64 | 48                     |
|                                               128 | 46                     |
|                                               256 | 46                     |
|                                           default | 46                     |
```

The general command is: `hadoop jar fifc.jar InputFormatCombineFile -D mapreduce.input.fileinputformat.split.minsize=64 nyse/split/input nyse/split/128/output`

### Use `TextInputFormat`

Comment this line `job.setInputFormatClass(CombineTextInputFormat.class);` in the above code so that hadoop uses the default input format (TextInputFormat.class). This map-reduce cannot even finishes in my hadoop server (AWS EC2 t2.medium with 4G RAM) becuase of 'GC overhead limit exceeded'. Here is the error message.
```
ubuntu@ip-172-31-25-109:~/documents/info7250/src$ hadoop jar fifc.jar InputFormatCombineFile nyse/split/input nyse/split/default/output
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
>FixedLengthInputFormat is an input format used to read input files which contain fixed length records. The content of a record need not be text. It can be arbitrary binary data. Users must configure the record length property by calling: FixedLengthInputFormat.setRecordLength(conf, recordLength);
or conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength);

## Input
The input data is a list of files in the current linux operation system (`sudo ls -R -l / | grep "^-" > files.txt`) with the follow format.
```
-rwxr-xr-x 1 root root 1037528 May 16  2017 bash
-rwxr-xr-x 1 root root  520992 Jun 15  2017 btrfs
-rwxr-xr-x 1 root root  249464 Jun 15  2017 btrfs-calc-size
-rwxr-xr-x 1 root root  278376 Jun 15  2017 btrfs-convert
-rwxr-xr-x 1 root root  249464 Jun 15  2017 btrfs-debug-tree
-rwxr-xr-x 1 root root  245368 Jun 15  2017 btrfs-find-root
-rwxr-xr-x 1 root root  270136 Jun 15  2017 btrfs-image
-rwxr-xr-x 1 root root  249464 Jun 15  2017 btrfs-map-logical
-rwxr-xr-x 1 root root  245368 Jun 15  2017 btrfs-select-super
-rwxr-xr-x 1 root root  253816 Jun 15  2017 btrfs-show-super
```
Now I want to know how many files in every permission property, which represented by the first 10 chars each line. Some of the them are like:
-rwxr-xr-x
-rwxr-xr--
-rwxr--r--
-rwxrw-r--
...

Here is the steps to clean the input data.
* Get the first columns with file permissions: `awk -F" " '{print $1}' files.txt > permissions.txt`
* Remove the new line in the permissions.txt using vim: `:%s/\n/`
* Create the input folder and then upload the file to HDFS: `fs -put permissions.txt inputformat/fixedlength/input/`
* Check and truncate the input file to the integer times of token size (9):  
```
fs -count inputformat/fixedlength/input/permissions.txt
0            1            3882708 inputformat/fixedlength/input/permissions.txt
```
Its length `3882708` is not the time of token length 9, so get ride of the last 'Partial record'.
Note: I don't understand the reason why the last record is not full yet.
```
fs -truncate 3882708 inputformat/fixedlength/input/permissions.txt
```

## Map Reduce Job
Here is the java program.
```java
/**
 * Demo: FixedLengthInputFormat
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InputFormatFixedLength extends Configured implements Tool {

    private static final IntWritable ONE = new IntWritable(1);

    public static class DemoMapper extends Mapper<Object, BytesWritable, Text, IntWritable> {
        private Text word = new Text();

        @Override
        public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
            word.set(value.getBytes());
            context.write(word, ONE);
        }
    }

    public static class DemoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int counter = 0;
            for (IntWritable i : values) {
                counter += i.get();
            }
            context.write(key, new IntWritable(counter));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // public static final String FIXED_RECORD_LENGTH
        // "fixedlengthinputformat.record.length"
        // FixedLengthInputFormat.setRecordLength(conf, 10);

        Job job = Job.getInstance(conf, "File Permissions");
        job.setJarByClass(InputFormatFixedLength.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(FixedLengthInputFormat.class);

        job.setMapperClass(DemoMapper.class);
        job.setReducerClass(DemoReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new InputFormatFixedLength(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
```

After compile and pack it as jar file, here is the command to run:
```
hadoop jar iffl.jar InputFormatFixedLength -D fixedlengthinputformat.record.length=9 inputformat/fixedlength/input inputformat/fixedlength/output
```
## Output

Here is the output file "part-r-00000".
```
--+-w-r--	1
---------	146095
--------r	546
--------w	315
-------r-	537
-------w-	1807
-------wx	1
------r--	543
------w--	58
------w-r	250
-----r--r	6328
-----w---	57
-----w--w	1
-----w-r-	252
----r--r-	7259
----w----	250
----w--w-	2
----w-r--	272
----w-rw-	2
---r-----	3
---r--r--	7455
---w-----	315
---w--w--	24
---w-r--r	1461
---w-rw-r	155
---wxr-x-	1
--r------	502
--r-----r	8438
--r----w-	177
--r---w--	80
--r---w-r	5112
--r--r---	6405
--w------	289
--w--w---	3
--w--w--w	42
--w--w-r-	21
--w-r--r-	3037
--w-rw-rw	380
-r-------	518
-r------w	9
-r-----r-	5462
-r----w--	199
-r---w---	78
-r---w-r-	1009
-r---w-rw	154
-r--r----	6412
-r--r---w	1812
-rw------	59
-rw----r-	95
-rw--w-rw	231
-rw-rw---	152
-rw-rw--w	229
-w-------	346
-w--w----	2
-w-r-----	15
-w-r--r--	49646
-w-rw-r--	31693
-w-rw-rw-	380
-wsr-xr-x	7
-wxr-xr-x	578
-wxrwxr-x	110
-wxrwxrwx	1
r--------	499
r------w-	15
r-----r--	5676
r----w---	158
r----w--w	21
r---w----	80
r---w-r--	1408
r---w-rw-	156
r---wxr-x	1
r--r-----	6560
r--r----w	171
r--r---w-	111017
r--r---ws	1
r--r---wx	732
r-sr-x-wx	11
r-x-w-r--	1
r-x-wxr-s	2
r-x-wxr-x	133
r-xr---wx	1
r-xr-x-w-	737
r-xr-x-ws	15
r-xr-x-wx	2262
rw-------	59
rw----r--	96
rw--w-rw-	231
rw-r---w-	12
rw-r---wx	3
rw-rw----	154
rw-rw--w-	231
w--------	371
w-----w-r	2
w----r--r	95
w--w-----	2
w--w--w--	21
w--w--w-r	21
w--w-r--r	21
w--w-rw-r	231
w-r--r---	1767
w-rw-----	57
w-rw----r	97
w-rw--w-r	231
w-rw-rw--	380
```

# KeyValueTextInputFormat

# NLineInputFormat

# SequenceFileInputFormat

# TextInputFormat
Please refer to `HW4 - Part 2.md`
