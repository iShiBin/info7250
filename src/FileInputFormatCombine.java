/**
 * Demo: CombineFileInputFormat
 */

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class FileInputFormatCombine extends Configured implements Tool {
    
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

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = this.getConf();
        
        Job job = Job.getInstance(conf, "Stock Average Price");
        job.setJarByClass(FileInputFormatCombine.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // job.setInputFormatClass(CombineFileInputFormat.class);
        
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        return job.waitForCompletion(true)?0:1;
    }
    
    /**
     * Implementing the Tool interface for MapReduce driver:
     * hadoop jar file.jar MainClass -D mapreduce.input.fileinputformat.split.maxsize=128 /input /output
     * @param args
     */
    public static void main (String[] args)  {

        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new FileInputFormatCombine(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}