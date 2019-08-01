
package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}



object TestLocal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestCluster")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    var rdd = sc.parallelize(1 to 8, 4)
    val pairdd = rdd.map(x => (x, x))
    val reducerdd = pairdd.reduceByKey(_ + _)
    val rrducerdd = reducerdd.reduceByKey(_ + _)
    val rrrducerdd = rrducerdd.sortByKey()
    val res = rrrducerdd.collect()
    println(res)
    while (true) {
      Thread.sleep(10);
    }
  }
}
