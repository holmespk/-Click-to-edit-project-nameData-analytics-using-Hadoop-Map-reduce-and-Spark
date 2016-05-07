object Problem_3b { 

def main(args: Array[String]) {
    val businessFile = sc.textFile(args(0))
    val reviewFile = sc.textFile(args(1))
   
    val bussinessRatingTuple = reviewFile.map(line => line.split("\\^")).map(line => (line(2),line(3).toDouble))

    val result = bussinessRatingTuple.combineByKey(
       (v:Double) => (v, 1),
       (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
       (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
       ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }

       val sortByKeys = result.sortBy(x => x._1)
       val topTen =sortByKeys.sortBy(x =>x._2,false).take(10)
       val businessIds = businessFile.map{ line => {
         val values = line.split("\\^")
        (values(0),values(1)+" "+values(2))
      }
    }
       val brBusinessInfo = sc.broadcast(businessIds.collectAsMap)
       
        val businessTop10AvgRatingArrayRDD = sc.parallelize(topTen)
                                            .coalesce(1,true)
                                            .map { rating => (rating._1 + " "+brBusinessInfo.value(rating._1)+" " +rating._2 )  }
                             
        businessTop10AvgRatingArrayRDD.foreach(x => println(x))  
   }
}

