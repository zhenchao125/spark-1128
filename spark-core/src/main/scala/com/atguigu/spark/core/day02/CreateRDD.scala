package com.atguigu.spark.core.day02

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 10:06
 */
object CreateRDD {
    def main(args: Array[String]): Unit = {
        // 1. 创建SparkContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("CreateRDD")
        val sc = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        //        val rdd: RDD[Int] = sc.parallelize(list1) // 一旦分区, 数据都到了executor上了.
        val rdd: RDD[Int] = sc.makeRDD(list1)
        val result: Array[Int] = rdd.collect()
        result.foreach(println)
        
        sc.stop()
        
    }
}

/*
通过集合得到RDD

 */