import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
        StockWritable stock = new StockWritable();
        Text exchange = new Text();
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            StockWritable stock = new StockWritable(value.toString());
//            exchange.set(stock.getExchange());
            
            String[] fields = value.toString().split(",");
            exchange.set(fields[0]);
            
            stock.setDate(fields[2]);
            stock.setVolume(Integer.parseInt(fields[7]));
            stock.setAdjClosePrice(Float.parseFloat(fields[8]));
            
            System.out.println(stock);
            context.write(exchange, stock); // key -> NullWritable.get()
        }
    }

    public static class TheReducer extends Reducer<Text, StockWritable, Text, StockWritable> {

        @Override
        public void reduce(Text key, Iterable<StockWritable> values, Context context)
                throws IOException, InterruptedException {

            int minVolume = Integer.MAX_VALUE;
            List<StockWritable> minVolumeStocks = new ArrayList<>();
            // could be more than one stock having the min Volume

            int maxVolume = Integer.MIN_VALUE;
            List<StockWritable> maxVolumeStocks = new ArrayList<>();
            // could be more than one

            float maxAdjClosePrice = 0;
            List<StockWritable> maxPriceStocks = new ArrayList<>();

            for (StockWritable stock : values) {

                int volume = stock.getVolume();
                float price = stock.getAdjClosePrice();

                if (volume < minVolume) {
                    minVolume = volume;
                    minVolumeStocks.clear();
                    minVolumeStocks.add(stock);
                } else if (volume == minVolume) {
                    minVolumeStocks.add(stock);
                }

                if (volume > maxVolume) {
                    maxVolume = volume;
                    maxVolumeStocks.clear();
                    maxVolumeStocks.add(stock);
                } else if (volume == maxVolume) {
                    maxVolumeStocks.add(stock);
                }

                if (price > maxAdjClosePrice) {
                    maxAdjClosePrice = price;
                    maxPriceStocks.clear();
                    maxPriceStocks.add(stock);
                } else if (price == maxAdjClosePrice) {
                    maxPriceStocks.add(stock);
                }
            }

            Collection<StockWritable> stocks = new ArrayList<StockWritable>();
            stocks.addAll(maxVolumeStocks);
            stocks.addAll(minVolumeStocks);
            stocks.addAll(maxPriceStocks);

            stocks.forEach(stock -> {
                try {
                    context.write(key, stock);
                } catch (IOException | InterruptedException e) {
                    System.err.println("Reduce failed. Cannot write to the output!");
                    e.printStackTrace();
                }
            });

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
//        job.setCombinerClass(TheReducer.class); // optimized
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