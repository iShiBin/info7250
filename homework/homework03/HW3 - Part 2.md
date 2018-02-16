### Copy the attached ‘access.log’ file into HDFS under /logs directory

- use `scp` command to copy local file to remote hadoop server
- `hadoop fs -copyFromLocal access.log` to put it to the HDFS (works in hadoop 3.0, in 2.x, applend the destination in the end of cmd)

### Implement MapReduce to find the number of times each IP accessed the website

#### Step 1: Create a Mapper class



public class MyMapper extends Mapper <Object key, Text value, Text, IntWritable> {

​    public void map(keyIn, valueIn, Context context) {

   

​        contxt.write(word, new IntWritable(1));

​    }

}

#### Step 2: Create a Reducer class

public class MyReducer extends Reducer () {

}

Step 3: Create a class with main method (driver class) that will create a JOB

public class Main {

​    public static void main(){

​        Job job;

​        job.setInutFormat();

​        job.setMMapperClass();

​        job.setReducerClass();

​       

​    }

}

#### Step 4: Compile the whole project to a jar package



#### Step 5: Run the hadoop jar command



