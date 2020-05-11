package com.atguigu.spark.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/11 8:58
 */
object Demo2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Demo2").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.textFile("c:/0508").collect
        sc.stop()
        
    }
}
