package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 10:05
 */
object ReduceByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List("hello" -> 1, "hello" -> 2, "world" -> 2, "world" -> 1, "hello" -> 2)
        val rdd1 = sc.parallelize(list1, 2)
        val rdd2 = rdd1.reduceByKey(_ + _)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}

/*
ReduceByKey
    聚合算子:
        只能用在kv形式的聚合.
        按照key进行聚合, 对相同的key的value进行聚合
        
        所有聚合类的算子都有预聚合
    
 */