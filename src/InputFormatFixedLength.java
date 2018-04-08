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