import org.apache.spark.{SparkConf, SparkContext}

object main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SBI").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val hyperGraphRdd = sc.textFile("C:\\Users\\raunol\\Desktop\\flights.csv").map(line => line.split(" "))

  }
}
