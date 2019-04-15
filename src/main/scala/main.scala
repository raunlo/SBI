import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Set
import scala.collection.mutable.ArrayBuffer

object main {
  def main(args: Array[String]): Unit = {
    val sc = sparkConf()
    val start = System.currentTimeMillis
    val items = findMT(sc)
    val totalTime = System.currentTimeMillis - start
    handleResults(items, totalTime)
  }

  def handleResults(items: Array[Set[String]], totalTime: Long): Unit = {
    println("Elapsed time: %1d ms".format(totalTime))
    items.foreach(x => println(x.foreach(print(_))))
    println(items.length)
  }

  def findMT(sc: SparkContext): Array[Set[String]] = {
    val hyperGraphRdd = sc.textFile("C:\\Users\\raunol\\Desktop\\SBI\\file.dat").map(line => line.split(" ")).zipWithIndex().map { case (x, y) => (y, x) }
    val itemList = hyperGraphRdd.flatMap(_._2.toList).map(x => (x, 1)).reduceByKey((x, y) => x + y).collect().map(x => (Set(x._1), Set[Long]()))
    val items = processItems(itemList, hyperGraphRdd.collect())
    items.toArray
  }

  def sparkConf(): SparkContext = {
    val conf = new SparkConf().setAppName("SBI").setMaster("local[2]").set("spark.executor.memory", "1g")
    new SparkContext(conf)
  }

  def filterOutEssentialConditionElements(newItemsArray: Array[(Set[String], Set[Long])], indexedHyperGraph: Array[(Long, Array[String])]): Array[(Set[String], Set[Long])] = {
    newItemsArray.foldLeft(Array[(Set[String], Set[Long])]()) {
      (acc, item) => computeItemSetDijSupport(item, acc, indexedHyperGraph)
    }
  }

  def computeItemSetDijSupport(itemSet: (Set[String], Set[Long]), filteredItemsSet: Array[(Set[String], Set[Long])], indexedHyperGraph: Array[(Long, Array[String])]): Array[(Set[String], Set[Long])] = {
    val itemDijSupport = computeSingleItemDijSupport(itemSet._1.last, indexedHyperGraph)
    val newIndexSet = itemDijSupport.union(itemSet._2)
    val essentialCondition = newIndexSet.size.equals(itemSet._2.size)
    if (!essentialCondition) {
      return filteredItemsSet :+ (itemSet._1, newIndexSet)
    }
    filteredItemsSet
  }

  def computeSingleItemDijSupport(item: String, hyperGraphRdd: Array[(Long, Array[String])]): Set[Long] = {
    hyperGraphRdd.filter(s => s._2.contains(item)).map(_._1).toSet
  }

  def findMax_cliqueItems(item: (Set[String], Set[Long]), lastIterationResult: Array[(Set[String], Set[Long])], indexedHyperGraph: Array[(Long, Array[String])], maxTransactions: Long): Array[(Set[String], Set[Long])] = {
    val itemsIndexes = lastIterationResult.flatMap(x => x._2).toSet
    val itemDijSupport = computeSingleItemDijSupport(item._1.last, indexedHyperGraph)
    val newItemIndexSet = itemDijSupport.union(item._2)
    if (!(newItemIndexSet.union(itemsIndexes).size == maxTransactions)) {
      return lastIterationResult :+ (item._1, item._2 ++ itemDijSupport)
    }
    lastIterationResult
  }

  def computeDijSupportForTo_exploreItem(item: (Set[String], Set[Long]), indexedHyperGraph: Array[(Long, Array[String])]): (Set[String], Set[Long]) = {
    val itemDijSupport = computeSingleItemDijSupport(item._1.last, indexedHyperGraph)
    (item._1, item._2 ++ itemDijSupport)
  }

  def compareIfMax_cliqueHaveItem(y: (Set[String], Set[Long]), max_clique: Array[(Set[String], Set[Long])]): Boolean = {
    !max_clique.exists(x => x._1.equals(y._1))
  }

  def addMax_cliqueMTLustIfNeeded(itemList: Array[ (Set[String], Set[Long])], maxTransactions: Long, MTList: ArrayBuffer[Set[String]]): ArrayBuffer[ Set[String] ]= {
      if (isMax_cliqueMT(itemList, maxTransactions)) {
        return MTList :+ itemList.flatMap(x => x._1).toSet
      }
      MTList
  }


  def isMax_cliqueMT(itemList: Array[ (Set[String], Set[Long])], maxTransactions: Long): Boolean = itemList.flatMap(x => x._2).toSet.size == maxTransactions


  def processItems(itemList: Array[(Set[String], Set[Long])], indexedHyperGraph: Array[(Long, Array[String])]): ArrayBuffer[Set[String]] = {
    val maxTransactions = indexedHyperGraph.length
    var MTArray = ArrayBuffer[Set[String]]()
    val max_clique = itemList.foldLeft(Array[(Set[String], Set[Long])]()) { (acc, item) => findMax_cliqueItems(item, acc, indexedHyperGraph, maxTransactions) }
    val to_explore = itemList.filter(y => compareIfMax_cliqueHaveItem(y, max_clique)).map(x => computeDijSupportForTo_exploreItem(x, indexedHyperGraph))
    MTArray =addMax_cliqueMTLustIfNeeded(max_clique, maxTransactions, MTArray)
    MTArray = MTArray ++ makeItemsForNextIteration_andCallNextIteration(to_explore, max_clique, maxTransactions, indexedHyperGraph)
    MTArray
  }

  def makeItemsForNextIteration_andCallNextIteration(to_explore: Array[(Set[String], Set[Long])], max_clique: Array[(Set[String], Set[Long])], maxTransactions: Long,
                                                     indexedHyperGraph: Array[(Long, Array[String])]): Array[Set[String]] = {
    to_explore.map(item => {
      getNewItemsAndMTArray(item, to_explore, max_clique, indexedHyperGraph, maxTransactions)
    }).flatMap(_.toList)
  }

  def getNewItemsAndMTArray(item: (Set[String], Set[Long]), to_explore: Array[(Set[String], Set[Long])], max_clique: Array[(Set[String], Set[Long])], indexedHyperGraph: Array[(Long, Array[String])]
                            , maxTransactions: Long): Array[Set[String]] = {
    val itemsWithDijSupport = getItemsWithDijSupport(to_explore, max_clique, item, indexedHyperGraph)
    val newItemArray = itemsWithDijSupport.filter(x => x._2.size != maxTransactions)
    itemsWithDijSupport.filter(i => i._2.size == maxTransactions).map(x => x._1) ++ processItems(newItemArray, indexedHyperGraph)
  }

  def getItemsWithDijSupport(to_explore: Array[(Set[String], Set[Long])], max_clique: Array[(Set[String], Set[Long])], item: (Set[String], Set[Long]), indexedHyperGraph: Array[(Long, Array[String])])
  : Array[(Set[String], Set[Long])] = {
    val newItemSet = getTo_exploreCombinationArray(to_explore, max_clique, item, indexedHyperGraph)
    filterOutEssentialConditionElements(newItemSet, indexedHyperGraph)
  }

  def getTo_exploreCombinationArray(to_explore: Array[(Set[String], Set[Long])], max_clique: Array[(Set[String], Set[Long])], item: (Set[String],
    Set[Long]), indexedHyperGraph: Array[(Long, Array[String])]): Array[(Set[String], Set[Long])] = {
    val index = to_explore.indexOf(item)
    val remainingArray = to_explore.slice(index + 1, to_explore.length)
    val newItemSet = max_clique.map(mc => (item._1 + mc._1.last, item._2)) ++ remainingArray.map(mc => (item._1 + mc._1.last, item._2))
    newItemSet
  }
}