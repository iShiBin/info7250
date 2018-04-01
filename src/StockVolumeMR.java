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
            stock = new StockWritable(value.toString());
            
            if (stock != null) {
                context.write(new Text(stock.getSymbol()), stock);
            } else {
                System.out.println("Map: StockWritable is null");
            }
        }
    }

    public static class TheReducer extends Reducer<Text, StockWritable, Text, StockWritable> {
        
        StockWritable max = new StockWritable();
        StockWritable min = new StockWritable();
        StockWritable price = new StockWritable(); // stock with max adjClosePrice

        @Override
        public void reduce(Text key, Iterable<StockWritable> values, Context context)
                throws IOException, InterruptedException {
            
            for(StockWritable stock: values) {
                
                int volume = stock.getVolume();
                
                if (max.getVolume() == -1 || max.getVolume() < volume) {
                    max = new StockWritable(stock);
                }
                
                if (min.getVolume() == -1 || min.getVolume() > volume) {
                    min.setDate(stock.getDate());
                    min.setVolume(volume);
                    min.setPrice(stock.getPrice());
                }
                
                if (price.getPrice() == -1 || price.getPrice() > stock.getPrice()) {
                    price = new StockWritable(stock);
                }
            }
            
            context.write(key, max);
            context.write(key, min);
            context.write(key, price);
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
        job.setCombinerClass(TheReducer.class);
        job.setReducerClass(TheReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StockWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
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