import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class IPDateGroupingComparator extends WritableComparator {

     public IPDateGroupingComparator() {
         super(IPDatePair.class, true);
     }

     @Override
     /**
      * This comparator controls which keys are grouped 
      * together into a single call to the reduce() method
      */
     public int compare(WritableComparable wc1, WritableComparable wc2) {
         IPDatePair pair = (IPDatePair) wc1;
         IPDatePair other = (IPDatePair) wc2;
         return pair.getIP().compareTo(other.getIP());
     }
 }