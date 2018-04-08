import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class IPDatePartitioner extends Partitioner<IPDatePair, Text> {
    @Override
    public int getPartition(IPDatePair pair, Text text, int numberOfPartitions) {
        // make sure that partitions are non-negative
        return Math.abs(pair.getIP().hashCode() % numberOfPartitions);
    }
}