/**
 * Demo: CombineFileInputFormat
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FileInputFormatCombine {
    
    private static final long MB = 1024 * 1024L;
    
    public static class StockMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        
        @Override
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
        
        @Override
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
        
        long maxSplitSize = MB * 128; //default value
        if(args.length>2) {
            maxSplitSize = MB * Long.parseLong(args[2]); // dynamic set the mapreduce.input.fileinputformat.split.maxsize
        }
//        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, maxSplitSize);
        
        Job job = Job.getInstance(conf, "Stock Average Price");
        job.setJarByClass(FileInputFormatCombine.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(CombineFileInputFormat.class);
        FileInputFormat.setMaxInputSplitSize(job, maxSplitSize); // another way to set SPLIT_MAXSIZE 
        
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
