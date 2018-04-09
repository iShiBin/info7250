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
