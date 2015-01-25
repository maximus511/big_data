var count = 1
var isTen = false
def averageRating[T](ratings: Iterable[T])(implicit avg: Numeric[T]) = {
	avg.toDouble(ratings.sum) / ratings.size
}
val movieMap = new scala.collection.mutable.HashMap[String,List[Int]].withDefaultValue(Nil)
val ratingsFile = sc.textFile("hdfs://localhost:9000/input/ratings.dat")
val ratings = ratingsFile.toArray
for(index <- 0 until ratings.length){
	val line = ratings(index).split("::")
			movieMap(line(1)) ::= line(2).toInt
}
val movieRatingMap = new scala.collection.mutable.HashMap[String,Double]
for((key,value) <- movieMap){
	val average = averageRating(value).toDouble
	movieRatingMap(key) = average
}
val sortedRatings = movieRatingMap.toSeq.sortBy(-_._2)
val iterator = sortedRatings.iterator
while(iterator.hasNext && !isTen){
	println(iterator.next())
	count = count + 1
	if(count == 10){
		isTen = true;
	}
}
