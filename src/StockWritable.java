/** 
 * HW5
 * 2. Create a Writable object that stores some fields from the the NYSE dataset to find
- the date of the max `stock_volume`
- the date of the min `stock_volume`
- the max `stock_price_adj_close`

  This will be a custom writable class with the above fields.
  Mapper will this object as a value, and Reducer will use this object as a value.
 */

/**
 * @author bin
 * @date 2018-03-20
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StockWritable implements Writable {
    private String exchange;
    private String symbol;
    private int date; // 2010-02-08 => 20100208
    private float openPrice, highPrice, lowPrice, closePrice, adjClosePrice;
    private int volume;
    

    public int getDate() {
        return this.date;
    }
    
    public void setDate(String date) {
        this.date = convert(date);
    }

    public int getVolume() {
        return this.getVolume();
    }
    
    public void setVolume(int volume) {
        this.volume = volume;
    }

    public float getAdjClosePrice() {
        return this.adjClosePrice;
    }
    
    public void setAdjClosePrice (float price) {
        this.adjClosePrice = price;
    }
    
    public String getExchange() {
        return this.exchange;
    }
    
    public void setExchange (String exchange) {
        this.exchange = exchange;
    }
    
    StockWritable() {
        this.exchange = "NYSE";
        this.symbol = "";
        this.date = 19700101;
        this.openPrice = 0f;
        this.highPrice = 0f;
        this.lowPrice = 0f;
        this.closePrice = 0f;
        this.adjClosePrice = 0f;
        this.volume = 0;
    }
    

    StockWritable(int date, int volume, float adjClosePrice) {
        this.date = date;
        this.volume = volume;
        this.adjClosePrice = adjClosePrice;
    }
    
    StockWritable(String line) { 
        this(); // initialize attributes in case of line is null
        
        //NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24
        /*
        | exchange              | NYSE      |
        | stock_symbol          | AEA       |
        | date                  | 2010-02-08|
        | stock_price_open      | 4.42      |
        | stock_price_high      | 4.42      |
        | stock_price_low       | 4.21      |
        | stock_price_close     | 4.24      |
        | stock_volume          | 205500    |
        | stock_price_adj_close | 4.24      |
         */
        
        String[] fields = line.split(",");
        if(fields == null || fields.length != 9) {
            System.out.println("The right input format is: "
                    + "exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close");
            return;
        }

        this.exchange = fields[0];
        this.symbol = fields[1];
        this.date = Integer.parseInt(removeDelimiter(fields[2]));

        this.openPrice = Float.parseFloat(fields[3]);
        this.highPrice = Float.parseFloat(fields[4]);
        this.lowPrice = Float.parseFloat(fields[5]);
        this.closePrice = Float.parseFloat(fields[6]);

        this.volume = Integer.parseInt(fields[7]);
        this.adjClosePrice = Float.parseFloat(fields[8]);
    }
    
    /**
     * This function will remove the delimiter ('-', '/', ':') from a date. 
     * @param date String
     * @return String without delimiter
     */
    private static String removeDelimiter(String date) { 
        StringBuffer buf = new StringBuffer();
        for(char ch: date.toCharArray()) {
            if(Character.isDigit(ch)) {
                buf.append(ch);
            }
        }
        return buf.toString();
    }
    
    private static int convert (String date) {
        return Integer.parseInt(removeDelimiter(date));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readInt();
        this.volume = in.readInt();
        this.adjClosePrice = in .readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // only write concerned fields
        out.writeInt(this.date);
        out.writeInt(this.volume);
        out.writeFloat(this.adjClosePrice);
    }

    @Override
    public String toString() {
        return this.date + "," + this.volume + "," + this.adjClosePrice;
    }
    
    public static void main(String[] args) throws IOException {
        String input = "NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24";
        StockWritable stock = new StockWritable(input);
        System.out.print(stock);
    }
}