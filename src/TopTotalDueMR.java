/**
 * Find the Top 10 Largest TotalDue Amount** (from the SalesOrder.csv file) using the Top 10 algorithms 
 * as explained in the lectures and demonstrated in the Lecture 08 slides.
 */
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopTotalDueMR extends Configured implements Tool {
    
    private static int k = 10;//dynamic k: hadoop jar topk.jar TopTotalDueMR -D topk=10 /input_dir /output_dir

    public static class RankMapper extends Mapper<Object, Text, NullWritable, DoubleWritable> {
        
//      PriorityQueue is better than TreeMap since TotalDue is the only value needed
        private PriorityQueue<Double> ranking = new PriorityQueue<>(k); // init size to k
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("topk", k); // default top 10
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            Double due = new Double(tokens[22]);
                        
            if(ranking.size() < k) {
                if (!ranking.contains(due)) ranking.add(due);
            } else if (ranking.peek() < due) {
                if (!ranking.contains(due)) { // same value won't be added twice
                    ranking.remove();
                    ranking.add(due);
                }
            }
        }
        
        @Override
        protected void cleanup (Context context) throws IOException, InterruptedException {
            for(Double due: ranking) {
                context.write(NullWritable.get(), new DoubleWritable(due));
            }
        }
    }

    public static class RankReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {

        private PriorityQueue<Double> ranking = new PriorityQueue<>(k); // init size to k

        @Override
        public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            
            for (DoubleWritable v: values) {
                double due = v.get(); 
                
                if(ranking.size() < k) {
                    if (!ranking.contains(due)) ranking.add(due);
                } else if (ranking.peek() < due) {
                    if (!ranking.contains(due)) { // same value won't be added twice
                        ranking.remove();
                        ranking.add(due);
                    }
                }
            }
            
            for(Double due: ranking) {
                context.write(NullWritable.get(), new DoubleWritable(due));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Top K Board");
        job.setJarByClass(TopTotalDueMR.class);

        job.setMapperClass(RankMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(RankReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        System.out.println("Get the top " + k +". You can set the k value: hadoop jar topk.jar TopTotalDueMR -D topk=10 /input_dir /output_dir");
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new TopTotalDueMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
