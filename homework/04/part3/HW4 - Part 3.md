# Requirements
Write one MapReduce program using each of the classes that extend FileInputFormat<k,v>
(CombineFileInputFormat, FixedLengthInputFormat, KeyValueTextInputFormat, NLineInputFormat, SequenceFileInputFormat, TextInputFormat)
http://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html

You could use any input file of your choice. The size of the input files is not important. The MR programs could simply do counting, or any other analysis you choose.

Note: The Hadoop version I am in use is 3.0.0 so I refer to http://hadoop.apache.org/docs/r3.0.0/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html
# CombineFileInputFormat
The typical scenario to utilize this `CombineFileInputFormat` is to solve [The Small Files Problem](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/) in Hadoop.

## Input

Split the whole NYSE data file to 9212 files, each of which is around 50K size, and has 1000 lines (except the last line). The command to use is `split -a 3 nyse_all_prices.csv` in Linux.

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
Run this command `hadoop jar fifc.jar InputFormatCombineFile nyse/split/input nyse/split/default/output`, and it takes 46'' to finish. (Refer to InputFormatCombineFile.console.output)

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
Note: Increase the memory should fix this problem but it is not this practice's concern, so I drop it from here.

## Output

The output is the same as part 2 in this homework: the average price of stock-price-high.

# FixedLengthInputFormat
>FixedLengthInputFormat is an input format used to read input files which contain fixed length records. The content of a record need not be text. It can be arbitrary binary data. Users must configure the record length property by calling: FixedLengthInputFormat.setRecordLength(conf, recordLength);
 r conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, recordLength);

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

The tail of the file is like this:
```
ubuntu@ip-172-31-25-109:~/downloads$ fs -tail inputformat/fixedlength/input/permissions.txt
-r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w--------w-r--r---w-r--r---w-r--r---w--------w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r--r---w-r------w-r------w--------w-r--r---w-r--r---w-r--r---w-r--r---w-r------w-r------w-rw-r---w-r------w-r------w-r------w-rw-r---w-r--r---w-r------w-r------w-r------w-r--
```

```
fs -count inputformat/fixedlength/input/permissions.txt
0            1            3882708 inputformat/fixedlength/input/permissions.txt
```

Its length `3882708` is not the time of token length 9, so get rid of the last 'Partial record'.
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
--+-w-r--    1
---------    146095
--------r    546
--------w    315
-------r-    537
-------w-    1807
-------wx    1
------r--    543
------w--    58
------w-r    250
-----r--r    6328
-----w---    57
-----w--w    1
-----w-r-    252
----r--r-    7259
----w----    250
----w--w-    2
----w-r--    272
----w-rw-    2
---r-----    3
---r--r--    7455
---w-----    315
---w--w--    24
---w-r--r    1461
---w-rw-r    155
---wxr-x-    1
--r------    502
--r-----r    8438
--r----w-    177
--r---w--    80
--r---w-r    5112
--r--r---    6405
--w------    289
--w--w---    3
--w--w--w    42
--w--w-r-    21
--w-r--r-    3037
--w-rw-rw    380
-r-------    518
-r------w    9
-r-----r-    5462
-r----w--    199
-r---w---    78
-r---w-r-    1009
-r---w-rw    154
-r--r----    6412
-r--r---w    1812
-rw------    59
-rw----r-    95
-rw--w-rw    231
-rw-rw---    152
-rw-rw--w    229
-w-------    346
-w--w----    2
-w-r-----    15
-w-r--r--    49646
-w-rw-r--    31693
-w-rw-rw-    380
-wsr-xr-x    7
-wxr-xr-x    578
-wxrwxr-x    110
-wxrwxrwx    1
r--------    499
r------w-    15
r-----r--    5676
r----w---    158
r----w--w    21
r---w----    80
r---w-r--    1408
r---w-rw-    156
r---wxr-x    1
r--r-----    6560
r--r----w    171
r--r---w-    111017
r--r---ws    1
r--r---wx    732
r-sr-x-wx    11
r-x-w-r--    1
r-x-wxr-s    2
r-x-wxr-x    133
r-xr---wx    1
r-xr-x-w-    737
r-xr-x-ws    15
r-xr-x-wx    2262
rw-------    59
rw----r--    96
rw--w-rw-    231
rw-r---w-    12
rw-r---wx    3
rw-rw----    154
rw-rw--w-    231
w--------    371
w-----w-r    2
w----r--r    95
w--w-----    2
w--w--w--    21
w--w--w-r    21
w--w-r--r    21
w--w-rw-r    231
w-r--r---    1767
w-rw-----    57
w-rw----r    97
w-rw--w-r    231
w-rw-rw--    380
```

# KeyValueTextInputFormat
Here is the description of this input format from API.
>An InputFormat for plain text files. Files are broken into lines. Either line feed or carriage-return is used to signal the end of a line. Each line is divided into key and value parts by a separator byte. If no such a byte exists, the key will be the entire line and value will be empty. The separator byte can be specified in config file under the attribute name mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default is the tab character ('\t').

## Input Data
Considering the following stock price data from different stock exchange (the first column is the stock exchange code), I would like to know how many stocks are there in each stock market.
>NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24
NASDAQ,AEA,2010-02-05,4.42,4.54,4.22,4.41,194300,4.41
NYSE,AEA,2010-02-04,4.55,4.69,4.39,4.42,233800,4.42
BSE,AEA,2010-02-03,4.65,4.69,4.50,4.55,182100,4.55
NYSE,AEA,2010-02-02,4.74,5.00,4.62,4.66,222700,4.66
NASDAQ,AEA,2010-02-01,4.84,4.92,4.68,4.75,194800,4.75
LSE,AEA,2010-01-29,4.97,5.05,4.76,4.83,222900,4.83
BSE,AEA,2010-01-28,5.12,5.22,4.81,4.98,283100,4.98
NASDAQ,AEA,2010-01-27,4.82,5.16,4.79,5.09,243500,5.09

So this dataset is separated by ',', and the key is the text before the first delimiter, and the rest of them are the value. This likes the `wordcount` example except the input format is `KeyValueTextInputFormat` so the key matters and value of the key don't in this case.

## MapReduce

```java
/**

 * Demo: KeyValueTextInputFormat
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InputFormatKeyValueText extends Configured implements Tool {

    private static final IntWritable ONE = new IntWritable(1);

    public static class DemoMapper extends Mapper<Text, Text, Text, IntWritable> {
        private Text word = new Text();
    
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            word.set(key);
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
    
        // "mapreduce.input.keyvaluelinerecordreader.key.value.separator"
    
        Job job = Job.getInstance(conf, "Stock Markets");
        job.setJarByClass(InputFormatKeyValueText.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
    
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
            code = ToolRunner.run(new Configuration(), new InputFormatKeyValueText(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}

```

## Execution
1. Compile: `hadoop com.sun.tools.javac.Main InputFormatKeyValueText.java `
2. Package: `jar cf ifkvt.jar InputFormatKeyValueText*.class`
3. Run: `hadoop jar ifkvt.jar InputFormatKeyValueText -D mapreduce.input.keyvaluelinerecordreader.key.value.separator="," inputformat/keyvalue/input inputformat/keyvalue/output`
Note: The "-D parameter=value" in this command is to set the config parameter using the Tools method, which is very convenient to adjust on different settings while keeping the program portable.

## Output
Here is the output of this job.
```
ubuntu@ip-172-31-25-109:~/documents$ fs -ls inputformat/keyvalue/output
Found 2 items
-rw-r--r--   1 ubuntu supergroup          0 2018-02-22 11:42 inputformat/keyvalue/output/_SUCCESS
-rw-r--r--   1 ubuntu supergroup         28 2018-02-22 11:42 inputformat/keyvalue/output/part-r-00000
ubuntu@ip-172-31-25-109:~/documents$ fs -cat inputformat/keyvalue/output/part-r-00000
BSE    2
LSE    1
NASDAQ    3
NYSE    3
```

# NLineInputFormat
>NLineInputFormat which splits N lines of input as one split. In many "pleasantly" parallel applications, each process/mapper processes the same input file (s), but with computations are controlled by different parameters.(Referred to as "parameter sweeps"). One way to achieve this is to specify a set of parameters (one set per line) as input in a control file (which is the input path to the map-reduce application, whereas the input dataset is specified via a config variable in JobConf.). The NLineInputFormat can be used in such applications, that splits the input file such that by default, one line is fed as a value to one map task, and a key is an offset. i.e. (k,v) is (LongWritable, Text). The location hints will span the whole mapred cluster.

A typical usage of this NLineInputFormat is to specify exactly how many lines should go to a mapper in order to control the map numbers instead of one for files small than the block size (128MB).

The constant field is `LINES_PER_MAP`, which could be set by    "mapreduce.input.lineinputformat.linespermap" config parameter.

## Input Data
The input data has `9211031` of lines, which could be divided to 149 with 61819 lines each. So the `mapreduce.input.lineinputformat.linespermap` will be set to `61819`.
```
ubuntu@ip-172-31-25-109:~/downloads$ wc -l nyse_all_prices.csv
9211031 nyse_all_prices.csv
```

## Map Reduce

```Java
/**

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

public class InputFormatNLine extends Configured implements Tool {

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
    
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Stock Average Price N Line");
        job.setJarByClass(InputFormatNLine.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(NLineInputFormat.class);
    
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
            code = ToolRunner.run(new Configuration(), new InputFormatNLine(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}

```

## Execution
1. Compile: `hadoop com.sun.tools.javac.Main InputFormatNLine.java `
2. Package: `jar cf ifnl.jar InputFormatNLine*.class`
3. Execute: `hadoop jar ifnl.jar InputFormatNLine -D mapreduce.input.lineinputformat.linespermap=61819 nyse/all/input inputformat/nline/output`

Here is some output from the console, which indicates there are 149 maps as expected.
>2018-02-22 12:41:39,591 INFO mapred.LocalJobRunner: 149 / 149 copied.
 018-02-22 12:41:39,592 INFO reduce.MergeManagerImpl: finalMerge called with 149 in-memory map-outputs and 0 on-disk map-outputs
 018-02-22 12:41:39,602 INFO mapred.Merger: Merging 149 sorted segments

## Output

```
ubuntu@ip-172-31-25-109:~/downloads$ fs -ls inputformat/nline/output
Found 2 items
-rw-r--r--   1 ubuntu supergroup          0 2018-02-22 12:41 inputformat/nline/output/_SUCCESS
-rw-r--r--   1 ubuntu supergroup      63817 2018-02-22 12:41 inputformat/nline/output/part-r-00000

ubuntu@ip-172-31-25-109:~/downloads$ fs -tail inputformat/nline/output/part-r-00000

WWW    18.248744274206285
WWY    54.36092631578994
WX    16.665575079872195
WXS    26.844325618515548
WY    35.7676935058714
WYN    22.293616071428584
XAA    13.422373765093061
XCJ    25.21970645792564
XCO    16.173618290258442
XEC    37.3061923733637
XEL    31.68209729905641
XFB    24.68197846567969
XFD    22.279146800501863
XFH    23.8809661229611
XFJ    24.449071518193215
XFL    25.74013801756589
XFP    20.425495608532025
XFR    24.208806896551696
XIN    6.20523020257827
XJT    7.756588235294107
XKE    8.034471896232267
XKK    9.14430991217063
XKN    17.724567126725265
XKO    10.411817731550283
XL    57.12595894804427
XOM    62.617120134360775
XRM    6.25682926829267
XRX    49.061969352329065
XTO    27.48834528933524
XVF    19.214903902154937
XVG    23.7846500672948
YGE    15.976983655274886
YPF    33.035930704897964
YSI    14.120734848484863
YUM    40.61453495830665
YZC    33.109506466984534
ZAP    11.207132887899105
ZEP    16.053975481611197
ZF    6.5832111410602305
ZLC    25.9632979749277
ZMH    61.70079662605441
ZNH    17.500465430016867
ZNT    25.719687669254405
ZQK    17.90524390243897
ZTR    7.388408084696904
ZZ    9.236293995859205
```

# SequenceFileInputFormat
SequenceFiles are flat files consisting of binary key/value pairs. SequenceFile provides SequenceFile.Writer, SequenceFile.Reader and SequenceFile.Sorter classes for writing, reading and sorting respectively.

There are three SequenceFile Writers based on the SequenceFile.CompressionType used to compress key/value pairs:

- Writer : Uncompressed records.
- RecordCompressWriter : Record-compressed files, only compress values.
- BlockCompressWriter : Block-compressed files, both keys & values are collected in 'blocks' separately and compressed. The size of the 'block' is configurable.

The actual compression algorithm used to compress key and/or values can be specified by using the appropriate CompressionCodec.

The recommended way is to use the static createWriter methods provided by the SequenceFile to chose the preferred format.

Here are some benefits to use this input format.
>Advantages:
 s binary files, these are more compact than text files rovides optional support for compression at different levels – record, block.
 files can be split and processed in parallel s HDFS and MapReduce are optimized for large files, Sequence Files can be used as containers for a large number of small files thus solving Hadoop's drawback of processing huge number of small files.
 extensively used in MapReduce jobs as input and output formats. Internally, the temporary outputs of maps are also stored using Sequence File format.

>And some limitations:
 similar to other Hadoop files, SequenceFiles are appended only.
 s these are specific to Hadoop, as of now, there is only Java API available to interact with sequence files. Multi-Language support is not yet provided.

Note: The above description is quoted from http://hadooptutorial.info/hadoop-sequence-files-example/

## Input
The input sequence file is as below:
```
ubuntu@ip-172-31-25-109:~/downloads$ fs -cat /inputformat/seq/input/log.seq
 EQ org.apache.hadoop.io.IntWritableorg.apache.hadoop.io.Text*org.apache.hadoop.io.compress.DefaultCodec?$b?Ҋ??S??d?P?e?????$b?Ҋ??S??d?P?e

?A??=VP@?uM
, ??>.????Lmx?s?wq
                  v    S    ??x??ѱ
?@
  ??W    ݛKbrmo?N*?M? ??">???$!??Y?TH>
                                     ?ω-??gbN???????
                                                    ź? ??????
??0????iUR???q̔??o???
```
The readable text version is as following using `fs -text /inputformat/seq/input/log.seq`
>ubuntu@ip-172-31-25-109:~/downloads$ fs -text /inputformat/seq/input/log.seq
    00    127.0.0.1 - - [15/Oct/2011:11:49:11 -0400] "GET / HTTP/1.1" 200 44
    01    127.0.0.1 - - [15/Oct/2011:11:49:11 -0400] "GET /favicon.ico HTTP/1.1" 404 209
    02    129.10.135.165 - - [15/Oct/2011:11:59:10 -0400] "GET / HTTP/1.1" 200 6
    03    129.10.135.165 - - [15/Oct/2011:11:59:10 -0400] "GET /favicon.ico HTTP/1.1" 404 209
    04    129.10.65.240 - - [15/Oct/2011:12:05:58 -0400] "GET / HTTP/1.1" 200 6
    05    129.10.65.240 - - [15/Oct/2011:12:05:58 -0400] "GET /favicon.ico HTTP/1.1" 404 209
    06    129.10.65.240 - - [15/Oct/2011:12:07:25 -0400] "GET /favicon.ico HTTP/1.1" 404 209
    07    146.115.62.108 - - [15/Oct/2011:12:57:44 -0400] "GET / HTTP/1.1" 200 6
    08    146.115.62.108 - - [15/Oct/2011:12:57:45 -0400] "GET /favicon.ico HTTP/1.1" 404 209
    09    146.115.62.108 - - [15/Oct/2011:12:57:45 -0400] "GET /favicon.ico HTTP/1.1" 404 209

It is actually the access log from a website, and the first column is the sequence key from 100 to 109.

## Map Reduce Program

The program is to count the access times from different IPs. As the key with `SequenceFileInputFormat` is the sequence number, I need to split the value instead and get the IP as the `key` for the reduce tasks.

Here is the source code in Java.
```java
// finename: InputFormatSequenceFile.java
/* Demo: SequenceFileInputFormat*/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InputFormatSequenceFile extends Configured implements Tool {

    private static final IntWritable ONE = new IntWritable(1);

    public static class DemoMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
        private Text ip = new Text();
    
        @Override
        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            ip.set(value.toString().split(" ")[0]);
            context.write(ip, ONE);
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
    
        Job job = Job.getInstance(conf, "InputFormatSequenceFile Demo");
        job.setJarByClass(InputFormatSequenceFile.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
    
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
            code = ToolRunner.run(new Configuration(), new InputFormatSequenceFile(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}

```
## Execution
After compiling and pack jar file, here is the command to run this map reduce job `hadoop jar ifsf.jar InputFormatSequenceFile /inputformat/seq/input/ /inputformat/seq/output`.

## Output
This job finishes successfully, and the result is right as below.
```bash
ubuntu@ip-172-31-25-109:~/downloads$ fs -ls /inputformat/seq/output
Found 2 items
-rw-r--r--   1 ubuntu supergroup          0 2018-02-23 12:08 /inputformat/seq/output/_SUCCESS
-rw-r--r--   1 ubuntu supergroup         62 2018-02-23 12:08 /inputformat/seq/output/part-r-00000

ubuntu@ip-172-31-25-109:~/downloads$ fs -cat /inputformat/seq/output/part-r-00000
127.0.0.1    2
129.10.135.165    2
129.10.65.240    3
146.115.62.108    3
```

# TextInputFormat
This is the Hadoop's default input format. The example is provided in `HW4 - Part 2.md`.