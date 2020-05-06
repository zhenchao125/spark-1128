package com.atguigu.spark.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 16:00
 */
object Test {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 5, 70, 6, 1, 2)
        val rdd1= sc.parallelize(list1, 2).map((_, 1))
        
        // 按照分区器分区, 每个分区内再按照key升序排列
        val rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(2))
        rdd2.glom().map(_.toList).collect().foreach(println)
        sc.stop()
        
    }
}
