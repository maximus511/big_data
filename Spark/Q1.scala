object Q1 {
   def q1func (input:Int) {
	val zipCode = input
	val data = sc.textFile("hdfs://localhost:9000/input/users.dat")
	val usersArray = data.toArray
	for(index <- 0 until usersArray.length){
		val line = usersArray(index).split("::")
		if(line(4) == zipCode.toString){
			 println(line(0))
		}
   	}
   }
}
