package com.atguigu.spark.core.day04.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/5/8 10:08
 */
object Reduce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val result = rdd1.reduce((x, y) => x + y)
    
        println(result)
        sc.stop()
        
    }
}
