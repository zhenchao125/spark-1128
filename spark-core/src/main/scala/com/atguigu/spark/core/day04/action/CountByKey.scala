package com.atguigu.spark.core.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 9:23
 */
object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 50, 70, 60, 10, 50, 10, 20)
        val rdd1 = sc.parallelize(list1, 2).map((_, null))
//        val rdd2: RDD[(Int, Int)] = rdd1.reduceByKey(_ + _)
        val rdd2 = rdd1.countByKey()
        println(rdd2)
        sc.stop()
        
    }
}
