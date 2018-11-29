val dataset = sc.textFile("/data/olympictweets2016rio")
val splitdataset = dataset.map(x => x.split(";")).map(x => if(x.length == 4 && x(2).length <= 140)x(2))
val athleteName = sc.textFile("/data/athletesrio.csv")
val header = athleteName.first()
val athleteNamefiltered = athleteName.filter(x => x!=header)
val splitathleteName = athleteNamefiltered.map(x => if(x.length >= 1)x.split(",")(1))
val collect = splitathleteName.collect()
val broadcastAthleteName = sc.broadcast(collect)


val container = splitdataset.map(x=> for(y <- broadcastAthleteName.value;if(x.toString().contains(y)))yield y)
val result = container.map(x => (x.toList,1)).reduceByKey(_+_)
val swap = result.map(x => (x._2,x._1))
swap.saveAsTextFile("result.txt")
