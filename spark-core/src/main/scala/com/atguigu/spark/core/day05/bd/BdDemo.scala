package com.atguigu.spark.core.day05.bd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/9 16:45
 */
object BdDemo {
    val arr = Array(30, 50, 10, 100, 300)
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("BdDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 3)
        
        val bdArr = sc.broadcast(arr)
        val rdd2 = rdd1.filter(x => bdArr.value.contains(x))
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}
