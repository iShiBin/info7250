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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class StockWritable implements Writable {

    private String symbol;
    private Date date;
    private int volume;
    private float price;

    public final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    String getSymbol() {
        return symbol;
    }

    void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    Date getDate() {
        return date;
    }

    void setDate(Date date) {
        this.date = new Date(date.getTime());
    }

    int getVolume() {
        return volume;
    }

    void setVolume(int volume) {
        this.volume = volume;
    }

    float getPrice() {
        return price;
    }

    void setPrice(float price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.date.getTime());
        out.writeInt(this.volume); // fixed bug: out.writeLong()
        out.writeFloat(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = new Date(in.readLong());
        this.volume = in.readInt();
        this.price = in.readFloat();
    }

    @Override
    public String toString() {
        return dateFormatter.format(date) + "\t" + volume + "\t" + price;
    }

    StockWritable() {
//        this.symbol = null;
        this.date = null;
        this.volume = -1;
        this.price = -1;
    }

    /**
     * Create a StockWritable instance from an input line of NYSE data set.
     * 
     * @param inputLine: line of input date, which has a format of:
     * exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
     * NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24
     * @return new instance of StockWritable;
     *         null if the format is not as expected
     */
    StockWritable(String inputLine) {
        
        String[] fields = inputLine.split(",");
        if (fields == null || fields.length != 9) {
            System.err.println("Bad data. The expected format is 9 columns seperated by comma.");
            return; // skip the bad data
        }

        try {
            this.symbol = fields[1];
            this.date = dateFormatter.parse(fields[2]);
            this.volume = Integer.parseInt(fields[7]);
            this.price = Float.parseFloat(fields[8]);
        } catch (Exception e) {
            System.err.println("Bad data. Cannot parse the symbol, date, volume and price in column 2, 3, 8 and 9.");
            e.printStackTrace();
            return;
        }
    }
    
    StockWritable(StockWritable stock) {
//        this.symbol = stock.symbol;
        this.date = new Date(stock.getDate().getTime());
        this.volume = stock.getVolume();
        this.price = stock.getPrice();
    }

    public static void main(String[] args) throws IOException {
        String input = "NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24";
        StockWritable stock = new StockWritable(input);
        System.out.print(stock);
    }
}