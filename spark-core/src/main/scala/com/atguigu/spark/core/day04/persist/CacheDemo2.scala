package com.atguigu.spark.core.day04.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 14:54
 */
object CacheDemo2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List("hello world", "hello")
        val rdd1 = sc.parallelize(list1, 2)
        
        val rdd2 = rdd1
            .flatMap(x => {
                println("flatMap ..." + x) // 2次
                x.split(" ")
            })
            .map(x => {
                println("map ..." + x) // 3次
                (x, 1)
            })
            .reduceByKey(_ + _)
        
        rdd2.cache()
        println("-------------")
        rdd2.collect
        println("-------------")
        rdd2.collect
        println("-------------")
        rdd2.collect
        
        Thread.sleep(1000000)
        sc.stop()
        
    }
}
