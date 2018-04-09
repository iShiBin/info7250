/**
 * Use MR Distinct pattern** to find the unique IP addresses from the http_access.log file provided in previous assignment. 
 * Use Combiner optimization if possible.
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IPDistinctMR extends Configured implements Tool {

    public static class TheMapper extends Mapper<Object, Text, Text, NullWritable> {
        private Text ip = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String token = value.toString().split(" ")[0];

            if (token == null) {
                return;
            }
            ip.set(token);
            context.write(ip, NullWritable.get());
        }
    }

    public static class TheReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // Write the user's id with a null value
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: IPDistinctMR <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(2);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Distinct IP");
        job.setJarByClass(IPDistinctMR.class);
        
        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);
        job.setCombinerClass(TheReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IPDistinctMR(), args);
        System.exit(res);
    }

}