import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;

public class MovieLensUtil {
    private static final String host = "localhost";
    private static final int port = 27017;
    private static final String databaseName = "lens";

    public static int importToMongo(String collectionName, String fileFullPath, String header, String delimeter) {

        MongoClient client = new MongoClient(host, port);
        MongoDatabase database = client.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        Document document = new Document();

        int counter = 0;
        Scanner data;
        try {
            data = new Scanner(new File(fileFullPath));
            if (header == null)
                header = data.nextLine();
            String[] names = header.split(delimeter);

            while (data.hasNextLine()) {
                String[] columns = data.nextLine().split(delimeter);
                for (int i = 0; i < names.length; i++) {
                    document.put(names[i], columns[i]);
                }
                collection.insertOne(document);
                // System.out.println(document);
                document.clear();
                counter++;
            }
            data.close();
        } catch (FileNotFoundException e) {
            counter = -1;
            e.printStackTrace();
        }

        client.close();

        return counter;
    }

    private static void loadRatings() {
        String header = "UserID::MovieID::Rating::Timestamp";
        String collection = "ratings";
        int n = importToMongo(collection, "/Volumes/FIT128G/Downloads/ml-1m/ratings.dat", header, "::");
        System.out.println("Successfully imported: " + n + " documents to " + collection);
    }

    private static void loadUsers() {
        String header = "UserID::Gender::Age::Occupation::Zip-code";
        String collection = "users";
        int n = importToMongo(collection, "/Volumes/FIT128G/Downloads/ml-1m/users.dat", header, "::");
        System.out.println("Successfully imported: " + n + " documents to " + collection);
    }

    private static void loadMovies() {
        String header = "MovieID::Title::Genres";
        String collection = "movies";
        int n = importToMongo(collection, "/Volumes/FIT128G/Downloads/ml-1m/movies.dat", header, "::");
        System.out.println("Successfully imported: " + n + " documents to " + collection);
    }

    public static void loadDataToMongo() {
        loadRatings();
        loadUsers();
        loadMovies();
    }

    public static void mapReduce(String inputCollection, String map, String reduce, String outputCollection) {
        MongoClient client = new MongoClient(host, port);
        DB database = client.getDB(databaseName);
        DBCollection collection = database.getCollection(inputCollection);
        
        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, outputCollection, MapReduceCommand.OutputType.REPLACE, null);
        
        MapReduceOutput it = collection.mapReduce(cmd);
        
        for (DBObject obj: it.results()) {
            System.out.println(obj.toString());
        }
        
        client.close();
    }
   
}
