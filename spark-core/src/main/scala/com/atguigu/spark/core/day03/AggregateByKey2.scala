package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 10:35
 */
object AggregateByKey2 {
    def main(args: Array[String]): Unit = {
        // 取出每个分区相同key对应值的最大值和最小值，然后最大值和最大值相加, 最小值和最小值相加
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        // zero 只在分区内聚合的使用
        
        rdd2.collect.foreach(println)
    }
}
