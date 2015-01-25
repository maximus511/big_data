REGISTER /home/004/r/rk/rkn130030/BigData/Pig/GenreFormat.jar;
Movie = load '/Fall2014_HW-3-Pig/movies_new.dat' using PigStorage('#') as (MOVIEID:int, TITLE:chararray, GENRES:chararray);
B = FOREACH Movie GENERATE MOVIEID, TITLE , GenreFormat(GENRES);
DUMP B;