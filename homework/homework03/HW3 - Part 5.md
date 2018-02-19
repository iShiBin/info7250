### PART 5  Requirement

Write a MapReduce to find the number of movies rated by different users in the MovieLens dataset.


### Input

All ratings are contained in the file "ratings.dat" and are in the following format:

> UserID::MovieID::Rating::Timestamp

- UserIDs range between 1 and 6040
- MovieIDs range between 1 and 3952
- Ratings are made on a 5-star scale (whole-star ratings only)
- Timestamp is represented in seconds since the epoch as returned by time(2)
- Each user has at least 20 ratings

The next step is to create according folder and update this file to HDFS similarly to Part 4.

>hadoop fs -mkdir movie
>
>hadoop fs -mkdir movie/input
>
>hadoop fs -put ratings.dat movie/input

Note: Supposing the movie is a new dir in the HDFS.

### Source Code

So the key is the `MovieID`, and the vlaue is `1` for the map reduce job, which is similar to part 4. The difference is the input file changes to `ratings.dat`.

```java
// file name: MovieCount.java
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

public class MovieCount {

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
    Job job = Job.getInstance(conf, "Movie Count");
    job.setJarByClass(MovieCount.class);
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

Compile, pack and run.

```bash
$hadoop com.sun.tools.javac.Main MovieCount.java
$jar cf movie.jar MovieCount*.class
$hadoop jar movie.jar MovieCount /user/ubuntu/movie/input /user/ubuntu/movie/output
```

### Output

Here is the first 10 lines of the output file part-r-00000. The whole file has been attached as `Movie Output.txt`

>1	2077
	0	888
	00	128
	000	20
	002	8
	003	121
	004	101
	005	142
	006	78
	007	232

### Verification

Use `grep` and `wc -l` to cross check.

```bash
$ grep "::1005::" ratings.dat | wc -l
142
$ grep "::1006::" ratings.dat | wc -l
78
$ grep "::1007::" ratings.dat | wc -l
232
```



