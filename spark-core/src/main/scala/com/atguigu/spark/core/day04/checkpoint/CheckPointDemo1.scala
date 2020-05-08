package com.atguigu.spark.core.day04.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 15:36
 */
object CheckPointDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setCheckpointDir("./ck1")
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
        
        rdd2.checkpoint()  // 仅仅是一个计划
        rdd2.cache()
        rdd2.collect
        println("-------------")
        rdd2.count()
        println("-------------")
        rdd2.collect
    
        Thread.sleep(100000)
        sc.stop()
    }
}
/*
checkpoint:
    如果rdd做checkpoint, 不会使用上个job结果.
    spark会自动启动一个新的job, 专门的去checkpoint
 */