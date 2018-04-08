/**
 * Using the MoviLens dataset, determine the median and standard deviation of ratings per movie.
 * Iterate through the given set of values and add each value to an in-memory list. 
 * The iteration also calculates a running sum and count.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MovieMedianStdDevMR extends Configured implements Tool {
    

    public static class TheMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        private Text movieID = new Text(); // Text is more generic than IntWritable
        private IntWritable rating = new IntWritable(); // whole-star ratings only

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("::");
            movieID.set(fields[1]);
            rating.set(Integer.parseInt(fields[2]));
            context.write(movieID, rating);
        }
    }    

    public static class TheReducer extends Reducer<Text, IntWritable, Text, MedianStdDevTuple> {

        private MedianStdDevTuple result = new MedianStdDevTuple();
        private List<Integer> ratings = new ArrayList<Integer>();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            ratings.clear(); // bugfix
            
            for(IntWritable v: values) {
                ratings.add(v.get());
            }
            
            result.setMedian((float)getMedian(ratings));
            result.setStdDev((float)getStdDev(ratings));
            
            context.write(key, result);
        }
        
        private double getMedian(List<Integer> values) {
            List<Integer> list = new ArrayList<Integer>(values);
            Collections.sort(list);
            
            int size = list.size();
            if (list.size()%2 == 0) {
                return (list.get(size/2) + list.get((size-1)/2)) / 2.0; 
            } else {
                return list.get(size/2);
            }
        }
        
        private double getStdDev(List<Integer> values) {
            double average = values.stream().mapToInt(i->i.intValue()).summaryStatistics().getAverage();
            
            double sum = 0;
            for(Integer v: values) {
                sum += Math.pow(v.intValue() - average, 2);
            }
            
            double stddev = 0;
            if(values.size() > 1) {
                stddev = sum / (values.size() - 1);
            }
            stddev = Math.sqrt(stddev);
            
            return stddev;
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Rating Median and StdDev");
        job.setJarByClass(MovieMedianStdDevMR.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TheMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(TheReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new MovieMedianStdDevMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}