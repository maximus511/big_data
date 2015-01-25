val itemsMap = new scala.collection.mutable.HashMap[String,List[String]].withDefaultValue(Nil)
val itemMatrix = sc.textFile("hdfs://localhost:9000/input/part-00000")
val items = itemMatrix.toArray
for(index <- 0 until items.length){
	val line = items(index).split("\\s+")
	val key = line(0)
	if(line.length > 1){
		val lineSplit = line(1).split(",")
		for( i <- 0 until lineSplit.length){
			val movieSplit = lineSplit(i).split(":")
			val movieId = movieSplit(0)
			itemsMap(key) ::= movieId
		}
	}
}
val userRatedList = new scala.collection.mutable.MutableList[String]
val ratingsFile = sc.textFile("hdfs://localhost:9000/input/ratings.dat")
val ratings = ratingsFile.toArray
val userId = "45"
for(index <- 0 until ratings.length) {
	val line = ratings(index).split("::")
	if(line(0) == userId && line(2) == "4") {
		userRatedList += line(1)
	}
}
val moviesMap = new scala.collection.mutable.HashMap[String,String]
val movieFile = sc.textFile("hdfs://localhost:9000/input/movies.dat")
val movies = movieFile.toArray
for(index <- 0 until movies.length) {
	val line = movies(index).split("::")
	moviesMap(line(0)) = line(1)
}
for(index <- 0 until userRatedList.length) {
	val movie = userRatedList(index)
	if(itemsMap.contains(movie)){
		print("\n\n\n")
		print(moviesMap(movie)+" -- ")
		for(i <- 0 until itemsMap(movie).length){
			print(moviesMap(itemsMap(movie)(i)))
			if(i != (itemsMap(movie).length-1)) {
				print(",")
			}
		}
	}
}
