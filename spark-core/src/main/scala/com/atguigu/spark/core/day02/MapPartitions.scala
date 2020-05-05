package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 10:23
 */
object MapPartitions {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapPartitions").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)  // 两个分区
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        
        /*val rdd2 = rdd1.mapPartitions(it => {
            // 执行两次, 每个分区执行一次
            it.map(_ * 2)
        })*/
        val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => it.map((_, index)))
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
