# Part 2 Requirements
Download and Copy all the files to a folder in HDFS (http://msis.neu.edu/nyse/)  
Write a Java Program to implement PutMerge as discussed in the class to merge the NYSE files in HDFS to find the average price of stock-price-high values for each stock using MapReduce on the single merged-file. Compare the running times of your original program doing MapReduce on multiple files to the modified version that merges all the files into a single file to perform MapReduce.

# Prepare the Input

The first step is to download and unzip the file.

## Merge Price Data
Then I used the following script to merge them into one huge file.
```bash
!/bin/bash
FILES=NYSE/NYSE_daily_prices_*.csv
for f in $FILES
do
    echo "Processing $f file..."
    # ls -l $f
    cat $f >> NYSE_all.csv
done
```

## Remove CSV Header
Noticing there are a lot of headers in the file, so I remove all of them.
```bash
grep -v "exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close" NYSE_all.csv > nyse_all_prices.csv
```

## Put Merged File to HDFS
Create the input folder `nyse/all/input` (valid path in Hadoop 3.0) in HDFS, and then put the local file to HDFS.
> Note: I am using Hadoop 3.0 so the path works fine even without the leading '/'.

# MapReduce Job

## Java Source Code
Here is my Java program, and it is also attached as a separate file as well.
```java
// file name: StockMR.java
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

public class StockMR {

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
```

## Execute Hadoop Job
1. Compile the source code: `hadoop com.sun.tools.javac.Main StockMR.java`
2. Pack to jar file: `jar cf stock.jar StockMR*.class`
3. Run this jar: `hadoop jar stock.jar StockMR nyse/all/input nyse/all/output`

# Generate the Output
Here is the command to check the output files: `fs -ls nyse/all/output`
>Found 2 items  
 rw-r--r--   1 ubuntu supergroup          0 2018-02-20 18:27 nyse/all/output/_SUCCESS  
 rw-r--r--   1 ubuntu supergroup      63853 2018-02-20 18:27 nyse/all/output/part-r-00000  

*Note: I've set alias `fs` to `Hadoop fs` command to save time.*

The output file is `part-r-00000`, which has been renamed to `StockMR-mapreduce-merged.output`, and attached.

The console output is quite long so please refer to the attached file `HW4 - Part 2.console.output`.

The start time is `2018-02-20 18:26:38,172`, and the finish time is `2018-02-20 18:27:03,807`. So the running time is `0:0:25,635`(25 seconds, 635 mill-seconds).

# Verification
To ensure the result is correct, I used aggregation query to crosscheck in the MongoDB.

```json
db.stocks.aggregate(
   [
     {
       $match: {stock_symbol: "AA"}
     },
     {
       $group:
         {
           _id: "$stock_symbol",
           avgAmount: { $avg: "$stock_price_high" }
         }
     }
   ]
)
```
After running on the same data set on some random stocks "AA", "BBVA" and "ZAP", all the returned value matches like below.
```
{ "_id" : "AA", "avgAmount" : 52.45968205467008 }  
{ "_id" : "BBVA", "avgAmount" : 23.42291127368224 }  
{ "_id" : "ZAP", "avgAmount" : 11.207132887899034 }
```

# Compare with Small Input Files
Nothing to change in the source code level except:
- remove the header from every price file
- put those files to an input folder in HDFS
- run the same jar using Hadoop but with different input and output parameters

The console output is attached as `HW4 - Part 2.seperated.files.console.output`. According to the console output, the running time is 25 seconds and 841 mill-seconds (2018-02-20 19:50:08,385 - 2018-02-20 19:49:42,544).

And the running result is attached as `StockMR-mapreduce-separate.output`

# Conclusion
- Result. The output result of the two methods are similar (to the 12 digits after the . )
- Running time. No obvious running time difference when the input file is merged or not even though theoretically, it should be faster when the input file is large. Maybe it is because the input size is relatively small (~500M < 4 blocks).
