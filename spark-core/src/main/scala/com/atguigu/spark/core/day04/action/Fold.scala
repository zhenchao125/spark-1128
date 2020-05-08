package com.atguigu.spark.core.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 10:13
 */
object Fold {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Fold").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        
        val result = rdd1.fold(1)(_ + _)  // 243*/
        
        val list1 = List("a", "b", "c", "d", "e", "f")
        val rdd1= sc.parallelize(list1, 3)
        
        val result = rdd1.fold("?")(_ + _)
        
        println(result)
        sc.stop()
        
    }
}
