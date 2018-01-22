/**
 * Use java driver to connect to remote mongodb
 * @author bin
 *
 */

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.*;

public class MongoTestApp {
    public static void main(String[] args){
        
//        Step 1: Establish a connection
        MongoClient client = new MongoClient("ip_address", 27107);
        
//        Step 2: Point a database
        MongoDatabase database = client.getDatabase("nysedb");
        
//        Step 3: Point to a collection
        MongoCollection<Document> collection = database.getCollection("stocks");
        
    }
}
