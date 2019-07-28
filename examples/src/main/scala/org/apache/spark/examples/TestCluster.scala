
package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}



object TestCluster {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestCluster")
    sparkConf.setMaster("spark://localhost:7077")
    val sc = new SparkContext(sparkConf)
    var rdd = sc.parallelize(1 to 8, 1)
    val pairdd = rdd.map(x => (x, x))
    val reducerdd = pairdd.reduceByKey(_ + _)
    val rrducerdd = reducerdd.reduceByKey(_ + _)
    val res = rrducerdd.collect()
    println(res)
    while (true) {
      Thread.sleep(10);
    }
  }
}
