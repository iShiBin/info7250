/**
 * Use java driver to connect to remote mongodb
 * @author bin
 *
 */

import org.bson.Document;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class Homework02 {
    public static void main(String[] args){
        
//        Step 1: Establish a connection
        MongoClient client = new MongoClient("localhost", 27017);
        
//        Step 2: Point a database
        MongoDatabase database = client.getDatabase("nysedb");
        
//        Step 3: Point to a collection
        MongoCollection<Document> collection = database.getCollection("stocks");
        
//        Step 4: Run a query
//        DBObject query = new BasicDBObject("");
        
    }
}
