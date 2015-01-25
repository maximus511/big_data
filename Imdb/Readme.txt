Homework-1

1. For first question, the corresponding JAR file is UserZipCode.jar.
To execute the JAR file, please run the following command-

hadoop jar UserZipCode.jar UserZipcode /users.dat /zipOutput 55455

Here UserZipcode is the class to be executed in the JAR (Source code attached).
55455 is example zipcode to run. 

To check output-

hdfs dfs -cat /zipOutput/part-r-00000


2. For second question, the corresponding JAR file is TopTenMovies.jar.
To execute the JAR file, please run the following command-

hadoop jar TopTenMovies.jar TopTenMovies /ratings.dat /output /sorted

Here TopTenMovies is the class to be executed in the JAR (Source code attached).
/output --> Folder for first Map Reduce output (Movie ID and average rating of the movie).
/sorted --> Folder for second Map Reduce output (Top ten rated Movie IDs)

To check output-

hdfs dfs -cat /sorted/part-r-00000
