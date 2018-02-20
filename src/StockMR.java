/**
 * 2. Download and Copy all the files to a folder in HDFS (http://msis.neu.edu/nyse/) 
   
   Write a Java Program to implement PutMerge as discussed in the class to merge the NYSE files in HDFS
   to find the average price of stock-price-high values for each stock using MapReduce on the single merged-file.
    
   Compare the running times of your original program doing MapReduce on multiple files 
   to the modified version that merges all the files to a single file to perform MapReduce.
   
   Sample data:
   exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
   NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author bin
 *
 */
public class StockMR {
    
    public static class StockMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        public void map (Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            Text symbol = new Text();
            symbol.set(tokens[1]);
            
            DoubleWritable price = new DoubleWritable();
            price.set(Double.parseDouble(tokens[4]));
            
            context.write(symbol, price);
        }
    }
    
    public static class StockReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        private DoubleWritable average = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            double sum = 0;
            int counter = 0;
            for(DoubleWritable price: values) {
                sum += price.get();
                counter += 1;
            }
            average.set(sum/counter);
            context.write(key, average);
        }
    }

    /**
     * @param args
     */
    public static void main (String[] args) throws Exception  {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Average Price");
        job.setJarByClass(StockMR.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

}
