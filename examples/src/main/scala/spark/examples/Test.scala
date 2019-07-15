package spark.examples

import spark._
import SparkContext._
class Test {

}
object Test{
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local", "Spark shell")
    var rdd = sc.parallelize(1 to 9)
    var pairdd = rdd.map(x => (x, x * x))
    var reducerdd = pairdd.reduceByKey(_ + _)
    reducerdd.collect()
  }
}
