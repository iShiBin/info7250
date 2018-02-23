**Due by 11:59pm on Mon, Feb. 26**

Part 1. READING ASSIGNMENT  
   Pairwise Document Similarity in Large Collections with MapReduce

Part 2. Download and Copy all the files to a folder in HDFS (http://msis.neu.edu/nyse/)  
Write a Java Program to implement PutMerge as discussed in the class to merge the NYSE files in HDFS to find the average price of stock-price-high values for each stock using MapReduce on the single merged-file.

Compare the running times of your original program doing MapReduce on multiple files to the modified version that merges all the files to a single file to perform MapReduce.

Part 3. Write one MapReduce program using each of the classes that extend FileInputFormat<k,v>
(CombineFileInputFormat, FixedLengthInputFormat, KeyValueTextInputFormat, NLineInputFormat, SequenceFileInputFormat, TextInputFormat)
http://hadoop.apache.org/docs/r3.0.0/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html
You could use any input file of your choice. The size of the input files is not important. The MR programs could simply do counting, or any other analysis you choose.

Part 4. Download the following dataset and Copy all the files to a folder in HDFS
MovieLens 10M - Stable benchmark dataset. 10 million ratings and 100,000 tag applications applied to 10,000 movies by 72,000 users.
http://grouplens.org/datasets/movielens

Write a MapReduce to find the top 25 rated movies in the movieLens dataset
