Homework-2

1. For first question, the corresponding JAR file is MapSideJoin.jar.
To execute the JAR file, please run the following command-

hadoop jar MapSideJoin.jar MapSideJoin /users.dat /ratings.dat /finalOutput 2503

Here MapSideJoin is the class to be executed in the JAR (Source code attached).
2503 is example movie Id to run. 

To check output-

hdfs dfs -cat /finalOutput/part-r-00000


2. For second question, the corresponding JAR file is ReduceSideJoin.jar.
To execute the JAR file, please run the following command-

hadoop jar ReduceSideJoin.jar ReduceSideJoin /ratings.dat /output1 /output2 /movies.dat /output

Here ReduceSideJoin is the class to be executed in the JAR (Source code attached).
/output1 --> Folder for first Map Reduce output (Movie ID and average rating of the movie).
/output2 --> Folder for second Map Reduce output (Top ten rated Movie IDs)
/output  --> Folder for final Reducer output (Top Ten Movie names and their ratings- sorted on their ratings)

To check output-

hdfs dfs -cat /output/part-r-00000
