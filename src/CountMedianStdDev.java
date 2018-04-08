import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountMedianStdDev {

    public static void main(String[] args) {
        
        CountMedianStdDev test = new CountMedianStdDev();
        
        Map<Integer, Integer> values = new HashMap<>();
        
        values.put(3, 1);
        values.put(2, 1);
        values.put(5, 1);
        
        System.out.println(test.getMedian(values));
        System.out.println(test.getStdDev(values));
    }
    
    private Float getMedian(Map<Integer, Integer> map) {
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
    private Integer getNth(Map<Integer, Integer> map, long n) {
        
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
    
    private double getAverage(Map<Integer, Integer> map) {
        long count = map.values().stream().mapToInt(Integer::valueOf).sum();
        long sum = map.entrySet().stream().mapToInt((e) -> e.getKey() * e.getValue()).sum();
        return 1.0 * sum/count;
    }

    private double getStdDev(Map<Integer, Integer> map) {
        if(map.size() < 2) return 0;
        
        double average = getAverage(map);

        double sum = map.entrySet().stream().mapToDouble( e -> ( Math.pow(e.getKey() - average, 2)) * e.getValue()).sum();
        long n = map.values().stream().mapToInt(Integer::valueOf).sum();

        double stddev = 0;
        if (n > 1) {
            stddev = Math.sqrt (sum / (n - 1));
        }
        
        return stddev;
    }

}
