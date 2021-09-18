package cn.byd.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by su.gaoming on 2021/9/16
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("mySparkTest")
    val sc: SparkContext = new SparkContext(conf)

    val wordRDD: RDD[String] = sc.textFile("file:///D:\\MyWork\\project\\prj_oa\\data\\word.txt")

    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5),1)

    /*val result: RDD[(String, Int)] = wordRDD.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(x => x._1,false)
    result.foreach(println)*/



  }

}
