### [HW1 - Due by 11:59pm on Sun. Jan. 21](https://northeastern.blackboard.com/webapps/assignment/uploadAssignment?content_id=_15993989_1&course_id=_2514278_1&group_id=&mode=view)

**PART 1. Reading Assignment**

Google has built a massively scalable infrastructure for its search engine and other applications to solve the problem at every level of the application stack. The goal was to build a scalable infrastructure for parallel processing of massive data. Google therefore created a full system that included a DFS, a column-family-oriented DB, a distributed coordination system, and a MapReduce algorithm.

Google published a series of papers explaining some of the key pieces of its infrastructure. The most important of these publications are as follows. Read the following papers, and write a short report for each paper.

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/gfs-sosp2003.pdf>

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/mapreduce-osdi04.pdf>

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/bigtable-osdi06.pdf>

  <http://static.googleusercontent.com/media/research.google.com/en/us/archive/chubby-osdi06.pdf>

** **

**PART 2. Reading Assignment**

  Chapter 1. A database for the modern web (MongoDB in Action)

  <http://proquest.safaribooksonline.com.ezproxy.neu.edu/9781617291609/kindle_split_010_html>

 

**PART 3. Programming Assignment**

Create a database for a Contact Management System in MongoDB.

You could use any attributes you like, first name, last name, email, phone, address, city, zip, etc.

Create 5 records (each with different attributes and values you choose – remember we said in the lecture that data coming from the web is semi-structured and sparse).

Then delete any one record of your choice.

Then update some information from any one of the records of your choice.

You could take the screenshots by pressing ALT + PRT SCRN every time you execute a command, and paste into a word document.

You could then submit this document.

 

**PART 4. Programming Assignment **

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