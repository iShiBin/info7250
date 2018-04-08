import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class MovieMedianStdDevMCR extends Configured implements Tool {

    public static class TheMapper extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("::");

            if (fields == null || fields.length < 2) return;

            Text movieID = new Text(fields[1]);
            Text rating = new Text(fields[2]);
            
            context.write(movieID, rating);
        }
    }

    public static class TheCombiner extends Reducer<Text, Text, Text, Text> {
        
        Map<String, Integer> map = new HashMap<>();
        
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            map.clear();
            
            for(Text v: values) {
                String rating = v.toString();
                map.merge(rating, 1, (exist, addon)->exist.intValue() + addon.intValue());
            }
            
            //value format: {3=1, 4=2, 5=1}
            context.write(key, new Text(map.toString()));
            
        }
    }

    public static class TheReducer extends Reducer<Text, Text, Text, MedianStdDevTuple> {
        
        MedianStdDevTuple result = new MedianStdDevTuple();
        Map<Integer, Integer> map = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            map.clear();
            
//          {3=1, 4=2, 5=1}
            for(Text v: values) {
                String flatMap = v.toString().substring(1, v.getLength()-1); // remove {}
                String[] entries = flatMap.split(", ");
                
                if(entries == null || entries.length == 0) return; // skipped the empty ones
                
                for(String e: entries) { // 3=1, 4=2, 5=1
                    String[] keyValue = e.split("=");
                    int rating = Integer.valueOf(keyValue[0]);
                    int num = Integer.valueOf(keyValue[1]);
                    map.merge(rating, num, (exist, addon) -> exist.intValue() + addon.intValue());
                }
            }
            
            result.setMedian(getMedian(map));
            result.setStdDev((float)getStdDev(map));
            
            context.write(key, result);
        }
    }

    public static Float getMedian(Map<Integer, Integer> map) {
        long count = map.values().stream().mapToInt(Integer::valueOf).sum();
        
        Integer lowMedian = getNth(map, (count+1)/2);
        Integer highMedian = getNth(map, -(count+1)/2);
        
        if (lowMedian !=null && highMedian != null) {
            return ( lowMedian + highMedian ) / 2.0f;
        }
        else return null;
    }
    
    /**
     * Find the k-th element (start from 1) in the frequency map <key, occurrence>
     * @param map: frequency map <key, occurrence>
     * @param k: forward if k > 0, backward if k < 0, the smallest key if k = 0
     * @return the k-th key in the map
     */
    public static Integer getNth(Map<Integer, Integer> map, long n) {
        
        List<Integer> keys = new ArrayList<Integer>(map.keySet());
        Collections.sort(keys);
        
        if( n<0 ) {
            Collections.reverse(keys); // backward
            n =-n;
        }
        
        for(Integer key: keys) {
            n -= map.get(key);
            if (n <= 0) {
                return key;
            }
        }
        
        return null;
    }
    
    public static  double getAverage(Map<Integer, Integer> map) {
        long count = map.values().stream().mapToInt(Integer::valueOf).sum();
        long sum = map.entrySet().stream().mapToInt((e) -> e.getKey() * e.getValue()).sum();
        return 1.0 * sum/count;
    }

    public static  double getStdDev(Map<Integer, Integer> map) {
        if(map.size() < 2) return 0;
        
        double average = getAverage(map);

        double sum = map.entrySet().stream().mapToDouble( e -> ( Math.pow(e.getKey() - average, 2)) * e.getValue()).sum();
        long n = map.values().stream().mapToInt(Integer::valueOf).sum();

        double stddev = 0;
        if (n > 1) {
            stddev = Math.sqrt(sum/(n-1));
        }
        
        return stddev;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Rating Median and StdDev (Optimized Memoery)");
        job.setJarByClass(MovieMedianStdDevMCR.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TheMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(TheCombiner.class);

        job.setReducerClass(TheReducer.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        int code = -1;
        try {
            code = ToolRunner.run(new Configuration(), new MovieMedianStdDevMCR(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(code);
    }
}