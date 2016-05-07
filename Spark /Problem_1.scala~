object Problem_1 {
    def main(args :Array[String]) {
       val businessFile = sc.textFile(args(0))
       val linesWithCity = businessFile.filter(line => line.contains(args(1)))
       val businessIds = linesWithCity.map (line => line.split("\\^")(0)).distinct.foreach(println)

   }
}

