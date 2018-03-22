import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockVolumeMR extends Configured implements Tool {

    public static class TheMapper extends Mapper<Object, Text, Text, StockWritable> {
        StockWritable stock = null;
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            System.out.println(value);
            stock = new StockWritable(value.toString());
            
            if (stock != null) {
                context.write(new Text(stock.getExchange()), stock); // key -> NullWritable.get()
            } else {
                System.out.println("Map: StockWritable is null");
                System.exit(1);
            }
        }
    }

    public static class TheReducer extends Reducer<Text, StockWritable, Text, StockWritable> {
        
        StockWritable max = null;
        StockWritable min = null;
        StockWritable price = null; // stock with max adjClosePrice

        @Override
        public void reduce(Text key, Iterable<StockWritable> values, Context context)
                throws IOException, InterruptedException {
            
            for(StockWritable stock: values) {
                if (min == null || stock.getVolume() < min.getVolume()) {
                    min = new StockWritable(stock.getDate(), stock.getVolume(), stock.getAdjClosePrice());
                }
                
                if (max == null || stock.getVolume() > max.getVolume()) {
                    max = stock;
                }
                
                if (price == null || stock.getAdjClosePrice() > price.getAdjClosePrice()) {
                    price = stock;
                }
            }
            
            if(max != null) context.write(key, max);
            if(min != null) context.write(key, min);
            if(price != null) context.write(key, price);
            else System.out.println("Reduce: Error - null");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Max Min Stock Volume Finder");
        job.setJarByClass(StockVolumeMR.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TheMapper.class);
        job.setCombinerClass(TheReducer.class); // optimized
        job.setReducerClass(TheReducer.class);
        
        job.setMapOutputKeyClass(Text.class); //NullWritable
        job.setMapOutputValueClass(StockWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class); //NullWritable
        job.setOutputValueClass(StockWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new StockVolumeMR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}