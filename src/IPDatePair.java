import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.WritableComparable;

public class IPDatePair implements WritableComparable <IPDatePair> {
    private String IP;
    private long timestamp; // transform date to timestamp for easy serialization
    
    IPDatePair(){
        this.IP = "";
        timestamp = 0;
    }
    
    IPDatePair(String IP, Date date) {
        this.setIP(IP);
        this.setDate(date);
    }

    String getIP() {
        return IP;
    }

    void setIP(String IP) {
        this.IP = IP;
    }

    long getDate() {
        return timestamp;
    }
    
    void setDate(Date date) {
        this.timestamp = date.getTime();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.IP = in.readUTF();
        this.timestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(IP);
        out.writeLong(timestamp);
    }

    @Override
    public int compareTo(IPDatePair other) {
        int compareValue = this.getIP().compareTo(other.getIP());
        if (compareValue == 0) {
            long compareDate = this.getDate() - other.getDate();
            if (compareDate == 0) compareValue = 0;
            else compareValue = compareDate < 0? -1:1;
        }
        
        return -1*compareValue;  // sort descending
    }
    
    @Override
    public String toString() {
        return this.IP + "\t" + new Date(this.timestamp).toString();
    }
}