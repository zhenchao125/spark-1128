package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 14:25
 */
object Distinct_1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 60, 10, 20, 60, 10, 2, 10, 20, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        // groupBy去重  没有预聚合, 所以效率有点低
        val rdd2 = rdd1.groupBy(x => x).map(_._1)
        println(rdd2.collect().mkString(", "))
        sc.stop()
        
    }
}

