package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 10:18
 */
object Map {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        
        // 得到一个新的RDD, 是现在数据的2倍
//        val rdd2 = rdd1.map(x => x * 2)
        val rdd2 = rdd1.map(_ * 2)
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}
