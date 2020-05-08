package com.atguigu.spark.core.day04.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 14:54
 */
object CacheDemo {
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
        // 对那些需要重复使用RDD做持久化:
        //        rdd2.persist()  // rdd2经过第一个行动算子之后, 会刚刚计算的结果默认缓存到内存
        // 一般只持久化到内存!!!
        //        rdd2.persist(StorageLevel.DISK_ONLY)
        rdd2.cache() // 缓存到内存
        rdd2.collect
        
        println("-------------")
        rdd2.collect
        println("-------------")
        rdd2.countByKey()
        
        Thread.sleep(100000)
        sc.stop()
        
    }
}
