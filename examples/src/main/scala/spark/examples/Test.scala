package spark.examples

import spark._
import SparkContext._
class Test {
  def getlist() = List("1")
}
object Test{
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local", "Spark shell")
    var rdd = sc.parallelize(1 to 8, 4)
    var pairdd = rdd.map(x => (x, x * x))
    var reducerdd = pairdd.reduceByKey(_ + _)
    var rrducerdd = reducerdd.reduceByKey(_ + _)
    var res = rrducerdd.collect()
    println(res)
    while (true){
      Thread.sleep(10);
    }
  }
}
