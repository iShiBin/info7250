

### PART 1: Reading Assignment

>  Chapter 5 - Queries and aggregation [Done]

>  Chapter 6 - Updates, atomic operations, and deletes [Done]

> Chapter 7 - Indexing and query optimization [Done]

### PART 2: Import the Entire NYSE Dataset

> Write a .bat/.sh to import the entire NYSE dataset (stocks A to Z) into MongoDB.
>
> NYSE Dataset Link: http://msis.neu.edu/nyse/

Here is my bash script for macOS.

```bash
!/bin/bash
FILES=NYSE/NYSE_daily_prices_*.csv
for f in $FILES
do
    echo "Processing $f file..."
    # ls -l $f
    mongoimport --db nysedb --collection stocks --type csv --headerline --file $f
done
```

The sample output is as below in the console.

```
Processing NYSE/NYSE_daily_prices_A.csv file...
2018-01-30T21:01:32.569-0800	connected to: localhost
2018-01-30T21:01:35.558-0800	[######..................] nysedb.stocks	10.2MB/39.1MB (26.0%)
2018-01-30T21:01:38.555-0800	[#############...........] nysedb.stocks	21.4MB/39.1MB (54.8%)
2018-01-30T21:01:41.555-0800	[###################.....] nysedb.stocks	32.5MB/39.1MB (83.1%)
2018-01-30T21:01:43.385-0800	[########################] nysedb.stocks	39.1MB/39.1MB (100.0%)
2018-01-30T21:01:43.385-0800	imported 735026 documents
```

Once it is finished, here is the collection count in the database from mongo shell console.

```bash
> show dbs;
admin        0.000GB
config       0.000GB
local        0.000GB
nysedb       0.694GB
> use nysedb
switched to db nysedb
> show collections
stocks
> db.stocks.count()
9211031
```

### PART 3: Find the Average Price of stock_price_high Values for Each Stock

> Use the NYSE database to find the average price of stock_price_high values for each stock using MapReduce in a Java application. But you could run the MapReduce the way we did in the class as well.

The natural way is to use javascript to solve this problem since mongo shell support javascript. One thing to  mention is to use the finalize method to calculate the average value since this operation `average` is to associative.

```javascript
let map = function () {
	emit(this.stock_symbol, {"price":this.stock_price_high, "count":1});
}

let reduce = function (key, values) {
  reducedVal = {price: 0, count: 0}
  
  values.forEach(function(v){
    reducedVal.price += v.price;
    reducedVal.count += v.count;
  });
  
  return reducedVal;
}

let average = function (key, reducedValue) {
  return reducedValue.price / reducedValue.count;
}

db.stocks.mapReduce(map, reduce, {out:"mr_stock_price_avg_each", finalize:average})
```

Here is the MapReduce output.
```json
{
	"result" : "mr_stock_price_avg_each",
	"timeMillis" : 94802,
	"counts" : {
		"input" : 9211031,
		"emit" : 9211031,
		"reduce" : 95075,
		"output" : 2853
	},
	"ok" : 1
}
```

And some sample documents in the generated collection.

```json
> db.mr_stock_price_avg_each.find().limit(3);
{ "_id" : NaN, "value" : 14.350822478600747 }
{ "_id" : "AA", "value" : 52.459682054670246 }
{ "_id" : "AAI", "value" : 10.518446478515234 }
```

**Verification**

In order to verify whether this is correct average, I used NoSQL aggragate to calculate the average stock price for stock with symol "AA". Here is the script.

```json
db.stocks.aggregate(
   [
     {
       $match: {stock_symbol: "AA"}
     },
     {
       $group:
         {
           _id: "$stock_symbol",
           avgAmount: { $avg: "$stock_price_high" }
         }
     }
   ]
)
```

The result is `52.45968205467008`, so it does match the value `52.459682054670246` calculated using mapreduce. (The precision is trivial.)

### PART 4

> Modify PART 3 by adding a finalizer to find out the average stock price of all stocks in the finalizer.

1. Have global variable `sumAll` to store the running sum for every occrance of a stock.
2. Counter the number of prices, and store in the countAll.
3. Return a list [avgEach, avgAll] in the finalize function.

Note: Counting the accumulated sum should be in the map function rather than in the reduce function because some stock may only appear **once** in the dataset, in which case the reduce funtion **won't** be called.

```javascript
let map = function () {
    sumAll += this.stock_price_high;
    countAll += 1
	emit(this.stock_symbol, {"price":this.stock_price_high, "count":1});
}

let reduce = function (key, values) {
  reducedVal = {price: 0, count: 0}
  
  values.forEach(function(v){
    reducedVal.price += v.price;
    reducedVal.count += v.count;
  });
  
  return reducedVal;
}

let avgAll = function (key, reducedValue) {
  return { avg: reducedValue.price / reducedValue.count, allAvg: sumAll/countAll }
}
db.stocks.mapReduce(map, reduce, {out:"mr_stock_price_avg_all", scope:{sumAll:0, countAll:0},finalize:avgAll})
```

Here is the output after running this MR job.

```json
{
	"result" : "mr_stock_price_avg_all",
	"timeMillis" : 99689,
	"counts" : {
		"input" : 9211031,
		"emit" : 9211031,
		"reduce" : 95219,
		"output" : 2853
	},
	"ok" : 1
}
```

And, query the generated document to find out the average price.

```json
> db.mr_stock_price_avg_all.find().limit(3)
{ "_id" : NaN, "value" : { "avg" : 14.350822478600747, "allAvg" : 29.021358777266897 } }
{ "_id" : "AA", "value" : { "avg" : 52.459682054670246, "allAvg" : 29.021358777266897 } }
{ "_id" : "AAI", "value" : { "avg" : 10.518446478515234, "allAvg" : 29.021358777266897 } }
```

To verify, I run the aggragate in mongo shell.

```json
> db.stocks.aggregate(
   [
     {
       $group:{
         _id: null,
         averageHighPrice: { $avg: "$stock_price_high" }
       }
     }
   ]
)
{ "_id" : null, "averageHighPrice" : 29.0213587773182 }
```

And the average value matches. (The trival difference is due to the float precision.)

### PART 5: MongoDB indexing.

>Most of the time, you⁬l want to declare your indexes before putting your application into production. This allows indexes to be built incrementally, as the data is inserted. But there are two cases where you might choose to build an index after the fact. The first case occurs when you need to import a lot of data before switching into production.
>
>For instance, you might be migrating an application to MongoDB and need to seed the database with user information from a data warehouse. You could create the indexes on your user data in advance, but doing so after you have imported the data will ensure an ideally balanced and compacted index from the start. This will also minimize the net time to build the index.
>
>Use the NYSE dataset to declare your indexes before putting your application into production.

Create the index first, and then import all the data to the database. I would like to create unique index on (stock_symbol, date) because it is frequently used.

```sql
> show dbs;
admin   0.000GB
config  0.000GB
local   0.000GB
> 
> use nysedb;
switched to db nysedb
> db.stocks.ensureIndex({"stock_symbol":1, "date":1}, {unique: true})
{
	"createdCollectionAutomatically" : true,
	"numIndexesBefore" : 1,
	"numIndexesAfter" : 2,
	"ok" : 1
}
```

To ensure the successful importing:

```json
> show dbs;
admin   0.000GB
config  0.000GB
local   0.000GB
nysedb  0.794GB
```

### PART 6: MongoDB Indexing.

> Insert the NYSE dataset into a new database. You may use the existing NYSE database created before. Now, create indexes on existing data sets.

To copy the database to a new one, I used `db.copyDatabase()` as below.

```json
> db.copyDatabase("nysedb", "nysedb_copy") 
{ "ok" : 1 }
> show dbs;
admin        0.000GB
config       0.000GB
local        0.000GB
nysedb       0.794GB
nysedb_copy  0.792GB
```

Before creating the indexes, I checked the indexes in the new database.

```json
> use nysedb_copy
switched to db nysedb_copy
> db.stocks.getIndexes()
[
	{
		"v" : 2,
		"key" : {
			"_id" : 1
		},
		"name" : "_id_",
		"ns" : "nysedb_copy.stocks"
	},
	{
		"v" : 2,
		"unique" : true,
		"key" : {
			"stock_symbol" : 1,
			"date" : 1
		},
		"name" : "stock_symbol_1_date_1",
		"ns" : "nysedb_copy.stocks"
	}
]
```

So it seems that the index has already been copied in the `db.copyDatabase()`. So I just created a new one on `stock_symbok` and `stock_volume`. The unique is set to `false` as it may have multiple records with these values.

```json
> db.stocks.ensureIndex({"stock_symbol":1, "stock_volume":1}, {unique: false})
{
	"createdCollectionAutomatically" : false,
	"numIndexesBefore" : 2,
	"numIndexesAfter" : 3,
	"ok" : 1
}
```

I also noticed it will take more time to import the dataset if I created the index before importing.

### PART 7: Programming Assignment

>Write a Java application to execute MapReduce to find each of the followings:
> - Task 1. MapReduce to find the top 25 rated movies in the [movieLens](http://files.grouplens.org/datasets/movielens/ml-1m.zip) dataset
> - Task 2. MapReduce to find the number of males and females in the movielens dataset
> - Task 3. MapReduce to find the number of movies rated by different users

Here is the java source code, most of which is self-explained.

```java
/** filename: MovieLensUtil.java
*/
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
        int n = importToMongo(collection, "/Volumes/FIT128G/Downloads/ml-1m/ratings.dat", header, "::"); // data file path
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

```

```java
/** filename: MovieLens.java
Write a Java application to execute MapReduce to find each of the followings:
> - Task 1. MapReduce to find the top 25 rated movies in the [movieLens](http://files.grouplens.org/datasets/movielens/ml-1m.zip) dataset
> - Task 2. MapReduce to find the number of males and females in the movielens dataset
> - Task 3. MapReduce to find the number of movies rated by different users
*/

public class MovieLens {
    
    public static void getTopRatedMovies() {
        String map = "function () { emit(this.MovieID, 1); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        
        String inputCollection = "ratings";
        String outputCollection = "topMovies";
        
        MovieLensUtil.mapReduce(inputCollection, map, reduce, outputCollection);
//        then use mongo-shell to find the top 25 since it is the best way to get enriched information including movie titles
    }
    
    public static void countByGender() {
        String map = "function () { emit(this.Gender, 1); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        
        String inputCollection = "users";
        String outputCollection = "countByGender";
        
        MovieLensUtil.mapReduce(inputCollection, map, reduce, outputCollection);
    }
    
    public static void countRatingsByUser() {
        String map = "function () { emit(this.UserID, 1); }";
        String reduce = "function (key, values) { return Array.sum(values); }";
        
        String inputCollection = "ratings";
        String outputCollection = "ratingsByUser";
        
        MovieLensUtil.mapReduce(inputCollection, map, reduce, outputCollection);
    }
    
    public static void main(String[] args) {
        MovieLensUtil.loadDataToMongo();
        getTopRatedMovies();
        countByGender();
        countRatingsByUser();
    }
}
```

Note: To show the top 25 highest rated movies, I used the following query:

> db.topMovies.find().sort( {value: -1} ).limit(25); 

And here is the running result.

```json
{ "_id" : "2858", "value" : 3428 }
{ "_id" : "260", "value" : 2991 }
{ "_id" : "1196", "value" : 2990 }
{ "_id" : "1210", "value" : 2883 }
{ "_id" : "480", "value" : 2672 }
{ "_id" : "2028", "value" : 2653 }
{ "_id" : "589", "value" : 2649 }
{ "_id" : "2571", "value" : 2590 }
{ "_id" : "1270", "value" : 2583 }
{ "_id" : "593", "value" : 2578 }
{ "_id" : "1580", "value" : 2538 }
{ "_id" : "1198", "value" : 2514 }
{ "_id" : "608", "value" : 2513 }
{ "_id" : "2762", "value" : 2459 }
{ "_id" : "110", "value" : 2443 }
{ "_id" : "2396", "value" : 2369 }
{ "_id" : "1197", "value" : 2318 }
{ "_id" : "527", "value" : 2304 }
{ "_id" : "1617", "value" : 2288 }
{ "_id" : "1265", "value" : 2278 }
Type "it" for more
> it
{ "_id" : "1097", "value" : 2269 }
{ "_id" : "2628", "value" : 2250 }
{ "_id" : "2997", "value" : 2241 }
{ "_id" : "318", "value" : 2227 }
{ "_id" : "858", "value" : 2223 }
```