package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/4 16:50
 */
object Hello {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个SparkContext(spark-shell中, 自动创建)  sc
        val conf = new SparkConf()
            .setAppName("Hello")
        val sc = new SparkContext(conf)
        // 2. 通过sc从数据源得到数据, 第一个RDD  (文件地址从main函数传递)
        val lineRDD: RDD[String] = sc.textFile(args(0))
        // 3. 对RDD做各种转换
        val wordCountRDD = lineRDD
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
        // 4. 执行一个行动算子(collect: 把每个executor中执行的结果, 收集到driver端)
        val arr: Array[(String, Int)] = wordCountRDD.collect
        arr.foreach(println)
        // 5. 关闭SparkContext
        sc.stop()
    }
}
