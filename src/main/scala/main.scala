import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SBI").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)


    sc.setLogLevel("Error")


    val hyperGraphRdd = sc.textFile("C:\\Users\\raunol\\Desktop\\SBI\\test.dat").map(line => line.split(" ")).zipWithIndex().map { case (x, y) => (y, x) }
    val itemList = hyperGraphRdd.flatMap(_._2.toList).map(x => (x, 1)).reduceByKey((x, y) => x + y).collect().sortBy { case (k, v) => (v, k) }.map(x => x._1).map(Array(_))
    val items = processItems(itemList, hyperGraphRdd)
  }

  def filterOutEssentialConditionElements(returnableItemList: Array[Array[String]], hyperGraphRdd: org.apache.spark.rdd.RDD[(Long, Array[String])]): Array[(Array[String], Array[Int])] = {
    var removeItemsSet = Array[Array[String]]()
    var itemsWithDijSupport = Array[(Array[String], Array[Int])]()
    returnableItemList.foreach(itemSet => {
      var b = Array[Int]()
      itemSet.foreach(item => {
        val e = hyperGraphRdd.filter(s => s._2.contains(item) && (!b.contains(s._1) || b.isEmpty))
        val v = b ++ e.map(_._1.toInt).collect()

        if (b.length.equals(v.length)) {
          removeItemsSet = removeItemsSet :+ itemSet
        }
        b = v
      })
      itemsWithDijSupport = itemsWithDijSupport :+ (itemSet, Array(b.length))
    })
    itemsWithDijSupport.filter(s => !removeItemsSet.contains(s._1)).sortBy { case (k, v) => (v.last, k.last) }
  }


  def computeItemDijSupport(itemSet: Array[String], hyperGraphRdd: org.apache.spark.rdd.RDD[(Long, Array[String])],
                            indexList: Array[Long]): Array[Long] = {
    var indexes = indexList
    itemSet.foreach(item => {
      val e = hyperGraphRdd.filter(s => s._2.contains(item) && !indexes.contains(s._1))
      indexes = indexes ++ e.map(_._1).collect()
    })

    indexes
  }

  def processItems(itemList: Array[Array[String]], hyperGraphRdd: org.apache.spark.rdd.RDD[(Long, Array[String])]): Array[Array[String]] = {
    var MTArray = ArrayBuffer[Array[String]]()
    val maxTransactions = hyperGraphRdd.count().toInt
    var indexList = Array[Long]()
    var max_clique = Array[Array[String]]()
    var to_explore = Array[Array[String]]()
    itemList.foreach(itemSet => {
      val tempIndexList = computeItemDijSupport(itemSet, hyperGraphRdd, indexList)
      if (tempIndexList.length == maxTransactions) {
        to_explore = to_explore :+ itemSet
      } else {
        max_clique = max_clique :+ itemSet
        indexList = tempIndexList
      }
    })
    MTArray = MTArray ++ a(to_explore, max_clique, maxTransactions, hyperGraphRdd)
    MTArray.toArray
  }

  def a(to_exploreArray: Array[Array[String]], max_clique: Array[Array[String]], maxTransactions: Long, hyperGraphRdd: org.apache.spark.rdd.RDD[(Long, Array[String])]  ): Array[Array[String]] = {
    var MTArray = ArrayBuffer[Array[String]]()
    val to_explore = to_exploreArray.sortBy(_.last).distinct
    to_explore.foreach(i => {
      var returnableItemList = max_clique.map(y => (i :+ y.last).distinct)
      val itemIndex = to_explore.indexOf(i)
      val remainingArray = to_explore.slice(itemIndex + 1, to_explore.length)
      returnableItemList = returnableItemList ++ remainingArray.map(y => i :+ y.last)
      val itemsWithDijSupport = filterOutEssentialConditionElements(returnableItemList, hyperGraphRdd)
      MTArray = MTArray ++ itemsWithDijSupport.filter(i => i._2.last == maxTransactions).map(x => x._1)
      val newItemArray = itemsWithDijSupport.filter(x => x._2.last != maxTransactions).map(x => x._1)
      MTArray = MTArray ++ processItems(newItemArray, hyperGraphRdd)
    })
    MTArray.toArray
  }
}