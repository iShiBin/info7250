### Requirements

Download the following dataset and Copy all the files to a folder in HDFS

MovieLens 1M - Stable benchmark dataset. 1 million ratings from 6000 users on 4000 movies.

<https://grouplens.org/datasets/movielens/1m/>

Write a MapReduce to find the number of males and females in the movielens dataset 

### Input

User information is in the file "users.dat" and is in the following format:

```
UserID::Gender::Age::Occupation::Zip-code

All demographic information is provided voluntarily by the users and is
not checked for accuracy.  Only users who have provided some demographic
information are included in this data set.

- Gender is denoted by a "M" for male and "F" for female
```

So, split the text input in each line using  `::`, and choose the 2nd token `Gender` as the key, and the value should be always 1 to count the number of males and females. 

The next step is to put `users.dat` to HDFS:

```
hadoop fs -mkdir movie
hadoop fs -mkdir movie/input
hadoop fs -put users.dat movie/input
```

### Source Code

The souce code file name is `GenderCount.java` as below.

```java
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

public class GenderCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().split("::")[1]); //split
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
    Job job = Job.getInstance(conf, "Gender Count");
    job.setJarByClass(GenderCount.class);
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

### Execution

Compile and generate the jar.

> hadoop com.sun.tools.javac.Main GenderCount.java
>
> jar cf gender.jar GenderCount*.class

Run the jar file:

> hadoop jar gender.jar GenderCount /user/ubuntu/movie/input /user/ubuntu/movie/output

### Output

```
ubuntu@ip-172-31-25-109:~/downloads/ml-1m$ hadoop fs -cat movie/output/part-r-00000
F	1709
M	4331
```

### Verification

Use `wc -l` to count the `F` and `M` in the users.dat file. The output matches the above mapreduce result.

```
ubuntu@ip-172-31-25-109:~/downloads/ml-1m$ grep F users.dat | wc -l
1709
ubuntu@ip-172-31-25-109:~/downloads/ml-1m$ grep M users.dat | wc -l
4331
```

