import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieMedianStdDevMCR extends Configured implements Tool {

    public static class TheMapper extends Mapper<Object, Text, Text, SortedMapWritable<IntWritable>> {
        
        private Text movieID = new Text(); // Text is more generic
        private IntWritable rating = new IntWritable(); // whole-star ratings
        public static final IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("::");

            if (fields == null || fields.length < 2)
                return;

            SortedMapWritable<IntWritable> map = new SortedMapWritable<IntWritable>();
            
            movieID.set(fields[1]);
            rating.set(Integer.parseInt(fields[2]));
            map.put(rating, ONE);
            
            context.write(movieID, map);
        }
    }

    public static class TheCombiner extends Reducer<Text, SortedMapWritable<IntWritable>, Text, SortedMapWritable<IntWritable>> {
        
        protected void reduce(Text key, Iterable<SortedMapWritable<IntWritable>> values, Context context)
                throws IOException, InterruptedException {
            
            context.write(key, merge(values));
        }
    }
    
    public static SortedMapWritable<IntWritable> merge(Iterable<SortedMapWritable<IntWritable>> values) {

        SortedMapWritable<IntWritable> outValue = new SortedMapWritable<IntWritable>();

        // merge the sorted map
        for (SortedMapWritable<IntWritable> v : values) {
            for (Entry<IntWritable, Writable> entry : v.entrySet()) {
                if(outValue.containsKey(entry.getKey())) {
                    int existing = ( (IntWritable)outValue.get(entry.getKey())).get();
                    int adding = ( (IntWritable)entry.getValue() ).get();
                    outValue.put(entry.getKey(), new IntWritable(existing+adding));
                } else {
                    outValue.put(entry.getKey(), entry.getValue());
                }
            }
        }
        
        return outValue;
    }

    public static class TheReducer extends Reducer<Text, SortedMapWritable<IntWritable>, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<SortedMapWritable<IntWritable>> values, Context context)
                throws IOException, InterruptedException {

            // MedianStdDevTuple result = new MedianStdDevTuple();
            
            for (SortedMapWritable<IntWritable> map : values) {
                String rating = "";
                String times = "";
                for (Map.Entry<IntWritable, Writable> entry : map.entrySet()) {
                    rating = entry.getKey().toString();
                    times = entry.getValue().toString();
                }
                context.write(key, new Text(rating + "->" + times));
            }
        }
    }
//    
//    public static class TheReducer extends Reducer<Text, SortedMapWritable<IntWritable>, Text, MedianStdDevTuple> {
//        
//        Map<Integer, Integer> map = new HashMap<>();
//        MedianStdDevTuple result = new MedianStdDevTuple();
//        
//        @Override
//        public void reduce(Text key, Iterable<SortedMapWritable<IntWritable>> values, Context context)
//                throws IOException, InterruptedException {
//            
//            map.clear();
//            
//            SortedMapWritable<IntWritable> mergedSortedMap = merge(values);
//            
//            for (IntWritable k: mergedSortedMap.keySet()) {
//                int v = ((IntWritable)mergedSortedMap.get(k)).get();
//                map.put(k.get(), v);
//            }
//
//            Float median = getMedian(map);
//            result.setMedian(median == null?0:median.floatValue());
//            result.setStdDev((float) getStdDev(map));
//
//            context.write(key, result);
//        }
//    }
    
//    public static class TheReducer extends Reducer<Text, SortedMapWritable<IntWritable>, Text, MedianStdDevTuple> {
//        
//        Map<Integer, Integer> map = new HashMap<>();
//        MedianStdDevTuple result = new MedianStdDevTuple();
//        
//        @Override
//        public void reduce(Text key, Iterable<SortedMapWritable<IntWritable>> values, Context context)
//                throws IOException, InterruptedException {
//            
//            map.clear();
//            
//            for(SortedMapWritable<IntWritable> v: values) {
//                for (Entry<IntWritable, Writable> entry : v.entrySet()) {
//                    int rating = entry.getKey().get();
//                    int count = ((IntWritable) entry.getValue()).get();
//                    
//                    Integer counted = map.get(rating);
//                    if (counted == null) {
//                        map.put(rating, count);
//                    } else {
//                        map.put(rating, count + counted.intValue());
//                    }
//                }
//            }
//
//            Float median = getMedian(map);
//            result.setMedian(median == null?0:median.floatValue());
//            result.setStdDev((float) getStdDev(map));
//
//            context.write(key, result);
//        }
//    }
    
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
        job.setMapOutputValueClass(SortedMapWritable.class);
//        job.setCombinerClass(TheCombiner.class);

        job.setReducerClass(TheReducer.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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