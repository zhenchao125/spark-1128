package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 13:59
 */
object GroupByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc
            .parallelize(Array("hello", "world", "atguigu", "hello", "are", "go"))
            .map((_, 1))
        
        
        
        val rdd2 = rdd1.groupByKey().mapValues(_.size)
//        val rdd2 = rdd1.groupBy(_._1).mapValues(_.map(_._2))
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}

/*
groupBy
    可以用于所有的RDD
    
groupByKey
    只能用于KV形式的RDD
 */