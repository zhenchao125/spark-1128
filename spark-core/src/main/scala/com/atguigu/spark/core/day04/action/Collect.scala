package com.atguigu.spark.core.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 9:00
 */
object Collect {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Collect").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        rdd1.filter(x => {
            println("filter " + x)
            x > 30
        }).map(x => {
            println("map " + x)
            x * x
        }).collect
        Ordering.Int.reverse
        
        sc.stop()
        
    }
}
