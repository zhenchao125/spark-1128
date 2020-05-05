package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 11:29
 */
object Glom {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Glom").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        val rdd2 = rdd1.glom()
        rdd2.collect.map(_.toList).foreach(println)
        sc.stop()
        
    }
}
