object Problem_3a { 

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
   val topTenRDD = sc.parallelize(topTen)

val businessIds = businessFile.map(line => line.split("\\^")).map(line => (line(0),(line(1),line(2))))
val finalList = topTenRDD.join(businessIds).map(t => (t._1,t._2._2._1,t._2._2._2)).coalesce(1,true)
    finalList.distinct.sortBy(_._1).foreach(println)
   }
}
