package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 11:35
 */
object GroupBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(3, 50, 7, 6, 1, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2: RDD[(Int, Iterable[Int])] =rdd1.groupBy(x => x % 2)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
