package com.atguigu.spark.core.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/5/5 14:05
 */
object Sample {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Sample").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 1, 2, 3, 4)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        val rdd2: RDD[Int] = rdd1.sample(true, 0.5, 1)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
