# [Homework 1](https://northeastern.blackboard.com/webapps/assignment/uploadAssignment?content_id=_15993989_1&course_id=_2514278_1&group_id=&mode=view)

## PART 1. Reading Assignment

Google has built a massively scalable infrastructure for its search engine and other applications to solve the problem at every level of the application stack. The goal was to build a scalable infrastructure for parallel processing of massive data. Google therefore created a full system that included a DFS, a column-family-oriented DB, a distributed coordination system, and a MapReduce algorithm.

Google published a series of papers explaining some of the key pieces of its infrastructure. The most important of these publications are as follows. Read the following papers, and write a short report for each paper.

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/gfs-sosp2003.pdf>

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/mapreduce-osdi04.pdf>

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/bigtable-osdi06.pdf>

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/chubby-osdi06.pdf>

## PART 2. Reading Assignment

  Chapter 1. A database for the modern web (MongoDB in Action)

  <http://proquest.safaribooksonline.com.ezproxy.neu.edu/9781617291609/kindle_split_010_html>

 

## PART 3. Programming Assignment

Create a database for a Contact Management System in MongoDB.

You could use any attributes you like, first name, last name, email, phone, address, city, zip, etc.

Create 5 records (each with different attributes and values you choose – remember we said in the lecture that data coming from the web is semi-structured and sparse).

Then delete any one record of your choice.

Then update some information from any one of the records of your choice.

You could take the screenshots by pressing ALT + PRT SCRN every time you execute a command, and paste into a word document.

You could then submit this document.

 

## PART 4. Programming Assignment

Create a collection called ‘games’. We’re going to put some games in it.

Add 5 games to the database. Give each document the following properties: name, genre, rating (out of 100)

If you make some mistakes and want to clean it out, use remove() on your collection.

- Write a query that returns all the games.
- Write a query to find one of your games by name without using limit().
- Use the findOne method. Look how much nicer it’s formatted!
- Write a query that returns the 3 highest rated games.
- Update your two favorite games to have two achievements called ‘Game Master’ and ‘Speed Demon’, each under a single key. Show two ways to do this. Do the first using update() and do the second using save(). Hint: for save, you might want to query the object and store it in a variable first.
- Write a query that returns all the games that have both the ‘Game Maser’ and the ‘Speed Demon’ achievements.
- Write a query that returns only games that have achievements. Not all of your games should have achievements, obviously.

You could take the screenshots by pressing ALT + PRT SCRN or Snipping Tool every time you execute a command, and paste into a word document. You could then submit this document

---

# Accomplishments

Below is my achievement of part 3 and part 4 since part 1 and 2 are reading assignments.

## Part 3: Contact Management System in MongoDB

Create a database for a Contact Management System in MongoDB.

You could use any attributes you like, first name, last name, email, phone, address, city, zip, etc.

Create 5 records (each with different attributes and values you choose – remember we said in the lecture that data coming from the web is semi-structured and sparse).

```javascript
[ec2-user@ip-172-26-14-215 ~]$ mongo
MongoDB shell version v3.6.2
connecting to: mongodb://127.0.0.1:27017
MongoDB server version: 3.6.2
Server has startup warnings: 
2018-01-22T19:10:32.824+0000 I STORAGE  [initandlisten] 
2018-01-22T19:10:32.824+0000 I STORAGE  [initandlisten] ** WARNING: Using the XFS filesystem is strongly recommended with the WiredTiger storage engine
2018-01-22T19:10:32.824+0000 I STORAGE  [initandlisten] **          See http://dochub.mongodb.org/core/prodnotes-filesystem
2018-01-22T19:10:33.181+0000 I CONTROL  [initandlisten] 
2018-01-22T19:10:33.181+0000 I CONTROL  [initandlisten] ** WARNING: Access control is not enabled for the database.
2018-01-22T19:10:33.181+0000 I CONTROL  [initandlisten] **          Read and write access to data and configuration is unrestricted.
2018-01-22T19:10:33.181+0000 I CONTROL  [initandlisten] 
> show dbs;
admin   0.000GB
config  0.000GB
local   0.000GB
nysedb  0.692GB
prefs   0.000GB
test    0.000GB
> use assignment
switched to db assignment
> db.contact.insert(
...   {
...   "first_name" : "James",
...   "last_name" : "Butt",
...   "company_name" : "Benton, John B Jr",
...   "address" : "6649 N Blue Gum St",
...   "city" : "New Orleans",
...   "county" : "Orleans",
...   "state" : "LA",
...   "zip" : 70116,
...   "phone1" : "504-621-8927",
...   "phone2" : "504-845-1427",
...   "email" : "jbutt@gmail.com",
...   "web" : "http://www.bentonjohnbjr.com"
...   }
... )
WriteResult({ "nInserted" : 1 })
> show collections
contact
> db.contact.findOne()
{
	"_id" : ObjectId("5a6653f7cfb44dcc667b3eff"),
	"first_name" : "James",
	"last_name" : "Butt",
	"company_name" : "Benton, John B Jr",
	"address" : "6649 N Blue Gum St",
	"city" : "New Orleans",
	"county" : "Orleans",
	"state" : "LA",
	"zip" : 70116,
	"phone1" : "504-621-8927",
	"phone2" : "504-845-1427",
	"email" : "jbutt@gmail.com",
	"web" : "http://www.bentonjohnbjr.com"
}
> db.contact.insert(
...   {
...   "first_name" : "Josephine",
...   "last_name" : "Darakjy",
...   "company_name" : "Chanay, Jeffrey A Esq",
...   "address" : "4 B Blue Ridge Blvd",
...   "city" : "Brighton",
...   "county" : "Livingston",
...   "state" : "MI",
...   "zip" : 48116,
...   "phone1" : "810-292-9388",
...   "phone2" : "810-374-9840",
...   "email" : "josephine_darakjy@darakjy.org",
...   "web" : "http://www.chanayjeffreyaesq.com"
...   }
... )
WriteResult({ "nInserted" : 1 })
> db.contact.insert(
...   {
...   "first_name" : "Art",
...   "last_name" : "Venere",
...   "company_name" : "Chemel, James L Cpa",
...   "address" : "8 W Cerritos Ave #54",
...   "city" : "Bridgeport",
...   "county" : "Gloucester",
...   "state" : "NJ",
...   "zip" : 8014,
...   "phone1" : "856-636-8749",
...   "phone2" : "856-264-4130",
...   "email" : "art@venere.org",
...   "web" : "http://www.chemeljameslcpa.com"
...   }
... )
WriteResult({ "nInserted" : 1 })
> db.contact.insert(
...   {
...   "first_name" : "Lenna",
...   "last_name" : "Paprocki",
...   "company_name" : "Feltz Printing Service",
...   "address" : "639 Main St",
...   "city" : "Anchorage",
...   "county" : "Anchorage",
...   "state" : "AK",
...   "zip" : 99501,
...   "phone1" : "907-385-4412",
...   "phone2" : "907-921-2010",
...   "email" : "lpaprocki@hotmail.com",
...   "web" : "http://www.feltzprintingservice.com"
...   }
... )
WriteResult({ "nInserted" : 1 })
> db.contact.insert(
...   {
...   "first_name" : "Donette",
...   "last_name" : "Foller",
...   "company_name" : "Printing Dimensions",
...   "address" : "34 Center St",
...   "city" : "Hamilton",
...   "county" : "Butler",
...   "state" : "OH",
...   "zip" : 45011,
...   "phone1" : "513-570-1893",
...   "phone2" : "513-549-4561",
...   "email" : "donette.foller@cox.net",
...   "web" : "http://www.printingdimensions.com"
...   }
... )
WriteResult({ "nInserted" : 1 })
> db.contact.count()
5
```

Then delete any one record of your choice.

```javascript
> db.contact.remove({first_name: "Donette"})
WriteResult({ "nRemoved" : 1 })
> db.contact.count() 
4
// query and verify the record has been removed
> db.contact.find({first_name: "Donette"})
>
```

Then update some information from any one of the records of your choice.

```java
> db.contact.find({first_name: "Lenna"}).pretty()
{
	"_id" : ObjectId("5a665424cfb44dcc667b3f02"),
	"first_name" : "Lenna",
	"last_name" : "Paprocki",
	"company_name" : "Feltz Printing Service",
	"address" : "639 Main St",
	"city" : "Anchorage",
	"county" : "Anchorage",
	"state" : "AK",
	"zip" : 99501,
	"phone1" : "907-385-4412",
	"phone2" : "907-921-2010",
	"email" : "lpaprocki@hotmail.com",
	"web" : "http://www.feltzprintingservice.com"
}
> db.contact.update({last_name: "Paprocki"}, {$set: { last_name: "Shi" }})
WriteResult({ "nMatched" : 1, "nUpserted" : 0, "nModified" : 1 })
> db.contact.find({first_name: "Lenna"}).pretty()
{
	"_id" : ObjectId("5a665424cfb44dcc667b3f02"),
	"first_name" : "Lenna",
	"last_name" : "Shi",
	"company_name" : "Feltz Printing Service",
	"address" : "639 Main St",
	"city" : "Anchorage",
	"county" : "Anchorage",
	"state" : "AK",
	"zip" : 99501,
	"phone1" : "907-385-4412",
	"phone2" : "907-921-2010",
	"email" : "lpaprocki@hotmail.com",
	"web" : "http://www.feltzprintingservice.com"
}
```

You could take the screenshots by pressing ALT + PRT SCRN every time you execute a command, and paste into a word document.

You could then submit this document.

## Part 4

Create a collection called ‘games’. We’re going to put some games in it.

Add 5 games to the database. Give each document the following properties: name, genre, rating (out of 100)

```javascript
> db.games.insert({ name: "Halo Wars 2 Avatar Store", genre: "Avatar", rating: 80 })
WriteResult({ "nInserted" : 1 })
> db.games.insert({ name: "DOA5 Last Round", genre: "Fighting", rating: 70 })
WriteResult({ "nInserted" : 1 })
> db.games.insert({ name: "Forza Horizon", genre: "Racing", rating: 75 })
WriteResult({ "nInserted" : 1 })
> db.games.insert({ name: "Call of Duty", genre: "Shooter", rating: 90 })
WriteResult({ "nInserted" : 1 })
> db.games.insert({ name: "Final Fantasy XIII", genre: "Role Playing", rating: 88 })
WriteResult({ "nInserted" : 1 })
> db.games.count()
5
```

If you make some mistakes and want to clean it out, use remove() on your collection.

- Write a query that returns all the games.

  ```javascript
  > db.games.find().pretty()
  {
  	"_id" : ObjectId("5a6658bccfb44dcc667b3f04"),
  	"name" : "Halo Wars 2 Avatar Store",
  	"genre" : "Avatar",
  	"rating" : 80
  }
  {
  	"_id" : ObjectId("5a665921cfb44dcc667b3f05"),
  	"name" : "DOA5 Last Round",
  	"genre" : "Fighting",
  	"rating" : 70
  }
  {
  	"_id" : ObjectId("5a66594fcfb44dcc667b3f06"),
  	"name" : "Forza Horizon",
  	"genre" : "Racing",
  	"rating" : 75
  }
  {
  	"_id" : ObjectId("5a665983cfb44dcc667b3f07"),
  	"name" : "Call of Duty",
  	"genre" : "Shooter",
  	"rating" : 90
  }
  {
  	"_id" : ObjectId("5a6659b6cfb44dcc667b3f08"),
  	"name" : "Final Fantasy XIII",
  	"genre" : "Role Playing",
  	"rating" : 88
  }

  ```

- Write a query to find one of your games by name without using limit().

  ```javascript
  > db.games.find({genre: "Role Playing"})
  { "_id" : ObjectId("5a6659b6cfb44dcc667b3f08"), "name" : "Final Fantasy XIII", "genre" : "Role Playing", "rating" : 88 }
  ```

- Use the findOne method. Look how much nicer it’s formatted!

  ```javascript
  > db.games.findOne()
  {
  	"_id" : ObjectId("5a6658bccfb44dcc667b3f04"),
  	"name" : "Halo Wars 2 Avatar Store",
  	"genre" : "Avatar",
  	"rating" : 80
  }
  ```

- Write a query that returns the 3 highest rated games.

  ```javascript
  > db.games.find().sort({rating:1}).limit(3).pretty()
  {
  	"_id" : ObjectId("5a665921cfb44dcc667b3f05"),
  	"name" : "DOA5 Last Round",
  	"genre" : "Fighting",
  	"rating" : 70
  }
  {
  	"_id" : ObjectId("5a66594fcfb44dcc667b3f06"),
  	"name" : "Forza Horizon",
  	"genre" : "Racing",
  	"rating" : 75
  }
  {
  	"_id" : ObjectId("5a6658bccfb44dcc667b3f04"),
  	"name" : "Halo Wars 2 Avatar Store",
  	"genre" : "Avatar",
  	"rating" : 80
  }
  ```

- Update your two favorite games to have two achievements called ‘Game Master’ and ‘Speed Demon’, each under a single key. Show two ways to do this. Do the first using update() and do the second using save(). Hint: for save, you might want to query the object and store it in a variable first.

  ```javascript
  The first way is to use udpate().
  > db.games.update(
    {name: "Final Fantasy XIII"}, 
    { $set: {"Game Master": 1, "Speed Demon": 1}})
  WriteResult({ "nMatched" : 1, "nUpserted" : 0, "nModified" : 0 })

  // The second way is to use save().
  > db.games.save( 
    { "_id" : ObjectId("5a66d84f8403511c839608e0"), 
     "name" : "Halo Wars 2 Avatar Store", 
     "genre" : "Avatar", "rating" : 80, 
     "Game Master" : 1, "Speed Demon" : 1 } )
  WriteResult({ "nMatched" : 1, "nUpserted" : 0, "nModified" : 1 })

  ```

- Write a query that returns all the games that have both the ‘Game Maser’ and the ‘Speed Demon’ achievements.

  ```javascript
  > db.games.find ( {"Game Master": 1, "Speed Demon": 1} )
  { "_id" : ObjectId("5a6659b6cfb44dcc667b3f08"), "name" : "Final Fantasy XIII", "genre" : "Role Playing", "rating" : 88, "Game Master" : 1, "Speed Demon" : 1 }
  { "_id" : ObjectId("5a66d84f8403511c839608e0"), "name" : "Halo Wars 2 Avatar Store", "genre" : "Avatar", "rating" : 80, "Game Master" : 1, "Speed Demon" : 1 }
  ```


- Write a query that returns only games that have achievements. Not all of your games should have achievements, obviously.

  ```javascript
  // set oen achievement to two other games
  > db.games.update({name: "DOA5 Last Round"}, { $set: {"Game Master": 1}})
  WriteResult({ "nMatched" : 1, "nUpserted" : 0, "nModified" : 1 })
  > db.games.update({name: "Forza Horizon"}, { $set: {"Speed Demon": 1}})
  WriteResult({ "nMatched" : 1, "nUpserted" : 0, "nModified" : 1 })

  // then verify the query
  > db.games.find ( { $or: [ { "Speed Demon": 1}, {"Game Master": 1} ] } )
  { "_id" : ObjectId("5a665921cfb44dcc667b3f05"), "name" : "DOA5 Last Round", "genre" : "Fighting", "rating" : 70, "Game Master" : 1 }
  { "_id" : ObjectId("5a66594fcfb44dcc667b3f06"), "name" : "Forza Horizon", "genre" : "Racing", "rating" : 75, "Speed Demon" : 1 }
  { "_id" : ObjectId("5a6659b6cfb44dcc667b3f08"), "name" : "Final Fantasy XIII", "genre" : "Role Playing", "rating" : 88, "Game Master" : 1, "Speed Demon" : 1 }
  { "_id" : ObjectId("5a66d84f8403511c839608e0"), "name" : "Halo Wars 2 Avatar Store", "genre" : "Avatar", "rating" : 80, "Game Master" : 1, "Speed Demon" : 1 }
  ```

  ​