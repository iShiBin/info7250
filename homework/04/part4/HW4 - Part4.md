# REQUIREMENTS (PART 4)
Download the following dataset and copy all the files to a folder in HDFS

MovieLens 10M - Stable benchmark dataset. 10 million ratings and 100,000 tag applications applied to 10,000 movies by 72,000 users.
http://grouplens.org/datasets/movielens

Write a MapReduce to find the top 25 rated movies in the movieLens dataset.

# ASSUMPTIONS
* The term "top 25 rated movies" means the first 25 movies sorted by the number of ratings for each movie.
* If there is a tie in the last movie, all the movies will be counted except the last movie in the top list.
* If there is a tie in the last movie of the top list, all the movies with the same number of rating will show.

For example:
Taking top 5 rated movies for example:
> Input: A:5, B:4, C:4, D:3, E:3, F:3, G:2
> Output:
A:5
B:4
C:4
D:3
E:3
F:3

**Note**: The format of the rating is MovieID:counts of rating

**Explanation:**
* Movie A has the highest number of rates: 5. So now 4 to go.
* Movie B and C are ties, so now 2 to go because they are counted 2 movies.
* Movie D is included because its number of the rating is 3. Now 1 seat is left.
* Movie E and F are included since they are ties with movie D.
* Movie G is not included since there are actually 6 movies in the top list now.
* So the output will display 6 movies instead of 5 due to the tie in the last movie.

# ANALYSIS AND DESIGN
1. Count the number of rating for each movie using one MapReduce job.
All ratings are contained in the file "ratings.dat" and are in the following format:

```
UserID::MovieID::Rating::Timestamp
- UserIDs range between 1 and 6040
- MovieIDs range between 1 and 3952
- Ratings are made on a 5-star scale (whole-star ratings only)
- Timestamp is represented in seconds since the epoch as returned by time(2)
- Each user has at least 20 ratings
```
So, I can use `MovieID` as the key and `1` as the value in the Hadoop job.
**Optimization**: Use combiner to count the number of rating to light the workload of reducing.

2. Use another Hadoop job to sort and select the top 25 MovieID.
The output of step 1 will be "MovieID  `Count of Ratings`". So step two to take it as input and use the `Count of Ratings` as the key instead to sort the count of ratings in descending order.

Then, the program will select from the top ratings and count how many in the top list (25).
- if the selected movie numbers in the top list is less than 25, continue
- select the next movie or all the movies if there is a tie
- update the counter of the top list
- continue from the beginning

3. Get the movie name information from the movies.dat using MovieIDs from step 2.
Movie information is in the file "movies.dat" and is in the following format: MovieID::Title::Genres

So, joining the MoviedID from step 2 with this file to get the movie name information. This could be done by using a movie hash map [movieid:name] in a Java program. However, I will just manually search and get the result for simplicity.

**Notes**
* Step 3 will combine with step 2.
* I will generalize this problem as 'Top K' by taking a parameter in step 2 in the implementation.

# JOB 1: COUNT THE NUMBER OF RATINGS

## SOURCE CODE
```java

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingCounter extends Configured implements Tool {

    private static final IntWritable ONE = new IntWritable(1);

    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(value.toString().split("::")[1]); // change delimiter and index here
            context.write(word, ONE);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int counter = StreamSupport.stream(values.spliterator(), true).mapToInt(IntWritable::get).sum();
            // the second param of StreamSupport.stream() determines whether it is a parallel Stream.

            context.write(key, new IntWritable(counter));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Counter");
        job.setJarByClass(RatingCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new RatingCounter(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
```

## Input

Put the 'ratings.dat' file to HDFS folder /movielens/ml-1m/rating/input.

## Execution

- Compile: (Eclipse will automatically compile and output the classes files in $PROJECT_ROOT/bin)
- Package: `jar cf rc.ar RatingCounter*.class`
- Run: `hadoop jar rc.jar RatingCounter /movielens/ml-1m/rating/input /movielens/ml-1m/rating/output`

## OUTPUT
The first 10 lines of the output are as below:
```
1    2077
10    888
100    128
1000    20
1002    8
1003    121
1004    101
1005    142
1006    78
1007    232
```
Please refer to the ratingcounter.dms for all the contents.

# JOB 2: GET THE TOP K
## SOURCE CODE
```java
//filename: TopK.java

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sun.security.krb5.Config;

public class TopK extends Configured implements Tool {

    private static int k = 25;//dynamic k: hadoop jar topk.jar TopK -D topk=10 /input_dir /output_dir

    public static class RankMapper extends Mapper<Text, Text, LongWritable, Text> {

        @Override
        public void map(Text item, Text vote, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(vote.toString())), item);
        }
    }

    public static class RankReducer extends Reducer<LongWritable, Text, Text, LongWritable> { //bugfix

        private int counter = 0;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("topk", k);
            counter = 0;
        }

        @Override
        public void reduce(LongWritable vote, Iterable<Text> items, Context context)
                throws IOException, InterruptedException {

            if (counter < k) { // display the ties at last item
                for (Text item : items) {
                    context.write(item, vote); // flip the key and value
                    counter++;
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Top K Board");
        job.setJarByClass(TopK.class);

        job.setMapperClass(RankMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        System.out.println("Get the top " + k +". You can set the k value: hadoop jar topk.jar TopK -D topk=10 /input_dir /output_dir");
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new TopK(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
```

## INPUT

The input would be the output of the first map reduce job file 'ratingcounter.dms'.

## EXECUTION
```bash
jar cf topk.jar TopK*.class
hadoop jar topk.jar TopK /movielens/ml-1m/rating/output /movielens/ml-1m/rating/topk
```
Note: To keep the input directory clean, the needless files '_SUCCESS' need to be deleted.

## OUTPUT
Here is the output of this map reduce job.
```
2858    3428
260    2991
1196    2990
1210    2883
480    2672
2028    2653
589    2649
2571    2590
1270    2583
593    2578
1580    2538
1198    2514
608    2513
2762    2459
110    2443
2396    2369
1197    2318
527    2304
1617    2288
1265    2278
1097    2269
2628    2250
2997    2241
318    2227
858    2223
```
Note: The first column is the MovieID, and the second one is the number of ratings.

# TOP 25 MOST RATED MOVIES
Here comes the list in ratings descending order.
```
2858::American Beauty (1999)::Comedy|Drama
260::Star Wars: Episode IV - A New Hope (1977)::Action|Adventure|Fantasy|Sci-Fi
1196::Star Wars: Episode V - The Empire Strikes Back (1980)::Action|Adventure|Drama|Sci-Fi|War
1210::Star Wars: Episode VI - Return of the Jedi (1983)::Action|Adventure|Romance|Sci-Fi|War
480::Jurassic Park (1993)::Action|Adventure|Sci-Fi
2028::Saving Private Ryan (1998)::Action|Drama|War
589::Terminator 2: Judgment Day (1991)::Action|Sci-Fi|Thriller
2571::Matrix, The (1999)::Action|Sci-Fi|Thriller
1270::Back to the Future (1985)::Comedy|Sci-Fi
593::Silence of the Lambs, The (1991)::Drama|Thriller
1580::Men in Black (1997)::Action|Adventure|Comedy|Sci-Fi
1198::Raiders of the Lost Ark (1981)::Action|Adventure
608::Fargo (1996)::Crime|Drama|Thriller
2762::Sixth Sense, The (1999)::Thriller
110::Braveheart (1995)::Action|Drama|War
2396::Shakespeare in Love (1998)::Comedy|Romance
1197::Princess Bride, The (1987)::Action|Adventure|Comedy|Romance
527::Schindler's List (1993)::Drama|War
1617::L.A. Confidential (1997)::Crime|Film-Noir|Mystery|Thriller
1265::Groundhog Day (1993)::Comedy|Romance
1097::E.T. the Extra-Terrestrial (1982)::Children's|Drama|Fantasy|Sci-Fi
2628::Star Wars: Episode I - The Phantom Menace (1999)::Action|Adventure|Fantasy|Sci-Fi
2997::Being John Malkovich (1999)::Comedy
318::Shawshank Redemption, The (1994)::Drama
858::Godfather, The (1972)::Action|Crime|Drama
```

# FURTHERMORE
This is out of the assignment's scope but what could be done as follow-ups are:

- [x] generalize the K so that this program could be used to count the top K

* implement the `[movieid:name]` hash map so it can display the movie name instead of id automatically
