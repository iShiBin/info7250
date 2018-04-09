/**
 * Count the number of times each type of error occurred in the error.log file (attached) using Hadoop custom Counters.
 */
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ErrorTypeCounterMR extends Configured implements Tool {

    public static class ErrorTypeCounterMRMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
        public static final String ERROR_TYPE_GROUP = "Error Counters";
        public static final String UNKNOWN_ERROR = "Unknown";
        private Set<String> errorType = new HashSet<>(Arrays.asList("File", "script", "HTTP", "method")); // case matters

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String message = value.toString();

            if (isError(message)) {
                boolean unknown = true;
                
                for (String error : errorType) {
                    if (message.contains(error)) {
                        context.getCounter(ERROR_TYPE_GROUP, error).increment(1);
                    }
                    unknown = false;
//                    break; // bug!
                }

                if (unknown) {
                    context.getCounter(ERROR_TYPE_GROUP, UNKNOWN_ERROR).increment(1);
                }
            }
        }

        public static boolean isError(String message) {
            if (message == null || message.isEmpty())
                return false;

            if (message.contains("[error]") && (message.contains("[client"))) {
                return true;
            }
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ErrorTypeCounterMR(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count # of Errors in a Type");
        job.setJarByClass(ErrorTypeCounterMR.class);
        job.setMapperClass(ErrorTypeCounterMRMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        boolean success = job.waitForCompletion(true);

        FileSystem.get(conf).delete(outputDir, true);
        return success ? 0 : 1;
    }
}