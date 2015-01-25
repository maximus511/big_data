Rating = load '/Fall2014_HW-3-Pig/ratings_new.dat' using PigStorage('#') as (USERID:int, MOVIEID:int, RATINGS:double, TIMESTAMP:int);
User = load '/Fall2014_HW-3-Pig/users_new.dat' using PigStorage('#') as (USERID:int, GENDER:chararray, AGE:int, OCCUPATION:chararray, ZIPCODE:chararray);
A= COGROUP Rating BY USERID , User BY USERID;
B = FOREACH A GENERATE FLATTEN($1),FLATTEN($2);
C = LIMIT B 11;
DUMP C;