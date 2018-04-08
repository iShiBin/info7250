import java.io.IOException;
import java.text.SimpleDateFormat;

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

/**
 * Determine the average stock_price_adj_close value by the year.
 * Choose an implementation in which a Reducer could be used as a Combiner.
 *  (discussed in the lecture, and available in the slides).
 * 
 * @author bin
 *
 */
public class StockPriceAverageMR extends Configured implements Tool {
    
    public final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public static class TheMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
        
        private IntWritable year = new IntWritable();
        private CountAverageTuple tuple = new CountAverageTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields == null || fields.length != 9) {
                System.err.println("Bad data. The expected format is 9 columns seperated by comma.");
                return; // skip the bad data
            }

            try {
                year.set(dateFormatter.parse(fields[2]).getYear() + 1900);
                float price = Float.parseFloat(fields[8]);
                tuple.setAverage(price);
                tuple.setCount(1);
            } catch (Exception e) {
                System.err.println("Bad data. Cannot parse the symbol, date, volume and price in column 2, 3, 8 and 9.");
                e.printStackTrace();
                return;
            }
            
            context.write(year, tuple);
        }
    }    

    public static class TheReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {

        private CountAverageTuple tuple = new CountAverageTuple();
        
        @Override
        public void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            long sum = 0;
            
            for (CountAverageTuple v: values) {
                count += v.getCount(); // total count
                sum += v.getCount() * v.getAverage(); // running sum
            }
            tuple.setCount(count);
            float averagePrice = (float)1.0*sum/count;
            tuple.setAverage(averagePrice);
            context.write(key, tuple);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Stock Annual Average");
        job.setJarByClass(StockPriceAverageMR.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TheMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CountAverageTuple.class);
        job.setCombinerClass(TheReducer.class);
        
        job.setReducerClass(TheReducer.class);
        job.setNumReduceTasks(1);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(CountAverageTuple.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new StockPriceAverageMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}
