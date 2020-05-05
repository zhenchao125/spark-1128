package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 14:56
 */
object Coalasce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Coalasce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
//        val rdd2: RDD[Int] = rdd1.coalesce(3, true)
        val rdd2 = rdd1.repartition(5)
        rdd2.collect
        println("------")
        println(rdd1.getNumPartitions)
        println(rdd2.getNumPartitions)
        
        sc.stop()
        
    }
}
/*
coalesce
    用来改变RDD分区数.
    coalesce 只能减少分区,不能增加分区. 为啥? 因为coalesce默认是不shuffle
    如果启动shuffle, 也可以增加分区.
    
    以后, 如果减少分区, 尽量不要shuffle, 只有增加的分区的时候才shuffle
    
    实际应用:
        如果减少分区就使用 coalesce
        如果是增加分区就是用 repartition
    
    
 */