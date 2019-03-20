import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object main {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SBI").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    var to_explore = Array[String]()
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("Error")


    val hyperGraphRdd = sc.textFile("path").map(line => line.split(" ")).zipWithIndex().map { case (x, y) => (y, x) }
    val maxTransactions = hyperGraphRdd.collect().length
    val itemList = hyperGraphRdd.flatMap(_._2.toList).map(x => (x, 1)).reduceByKey((x, y) => x + y).collect().sortBy { case (k, v) => (v, k) }

   to_explore = to_explore ++ firstIterationFindLastITem(hyperGraphRdd, maxTransactions, itemList)

    var table = Array[Array[String]]()
     for{
       x <- to_explore
          y <- itemList
     }yield Array(x, y._1)

    for(x <- to_explore; y <- itemList if (!x.equals(y._1))) yield Array(x, y._1)


  }

  def firstIterationFindLastITem(hyperGraphRdd: org.apache.spark.rdd.RDD[(Long, Array[String])], maxTransactions: Int, itemList: Array[(String, Int)]): Array[String] = {
    var result = Array[String]()
    var items = hyperGraphRdd.filter(g => g._2.contains(itemList(0)._1)).collect()
    var usedIndexes = items.map(x => x._1)
    for (elem <- itemList) {
      val newItems = hyperGraphRdd.filter(g => !usedIndexes.contains(g._1) && g._2.contains(elem._1)).collect()
      items = items.union(newItems)
      usedIndexes = usedIndexes ++ newItems.map(x => x._1)
      if (items.length == maxTransactions) {
        result = result ++ Array(elem._1)
      }
    }
    result
  }


}
