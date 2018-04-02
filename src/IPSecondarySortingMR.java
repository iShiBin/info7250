import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Using the access.log file stored in HDFS, implement MapReduce to find the
 * number of times each IP accessed the web site. Also, use SecondarySorting to
 * sort the values based on AccessDate in a Descending Order.
 * 
 * access.log 
 * 127.0.0.1 - - [15/Oct/2011:11:49:11 -0400] "GET / HTTP/1.1" 200 44
 * 127.0.0.1 - - [15/Oct/2011:11:49:11 -0400] "GET /favicon.ico HTTP/1.1" 404
 * 129.10.135.165 - - [15/Oct/2011:11:59:10 -0400] "GET / HTTP/1.1" 200 6
 * ...
 * 
 * ref: https://www.safaribooksonline.com/library/view/data-algorithms/9781491906170/ch01.html#mapleft_parenthesisright_parenthesis_for
 * 
 * @author bin
 * @created 2018-04-01
 */

public class IPSecondarySortingMR extends Configured implements Tool {
    
    public final static SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    public static class TheMapper extends Mapper<Object, Text, IPDatePair, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String IP = line.split(" ")[0];
            Date date = formatter.parse(line, new ParsePosition(line.indexOf('[') + 1));

            IPDatePair reducerKey = new IPDatePair(IP, date);
            context.write(reducerKey, one);
        }
    }    

    public static class TheReducer extends Reducer<IPDatePair, IntWritable, IPDatePair, LongWritable> {

        public void reduce(IPDatePair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            long timestamp = -1;
            long sum = 0;
            
            for (IntWritable val : values) {
                if (timestamp == -1) timestamp = key.getDate();
                sum += val.get();
            }
            
            IPDatePair pair = new IPDatePair(key.getIP(), new Date(timestamp)); 
            LongWritable  outputValue = new LongWritable(sum);
            context.write(pair, outputValue);
        }
    }
    
////    This reducer is to verify secondary sorting. 
//    public static class TheReducer extends Reducer<IPDatePair, IntWritable, IPDatePair, LongWritable> {
//        private static LongWritable one = new LongWritable(1);
//        public void reduce(IPDatePair key, Iterable<IntWritable> values, Context context)
//                throws IOException, InterruptedException {
//            for(IntWritable v: values) context.write(key, one);
//        }
//    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "IP Counter - Secondary Sorting");
        job.setJarByClass(IPSecondarySortingMR.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TheMapper.class);
        job.setMapOutputKeyClass(IPDatePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(IPDatePartitioner.class);
        job.setGroupingComparatorClass(IPDateGroupingComparator.class);        
        
        job.setReducerClass(TheReducer.class);
        job.setNumReduceTasks(1); // map only to verify the secondary sorting
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(IPDatePair.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new IPSecondarySortingMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }

}


