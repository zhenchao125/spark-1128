package com.atguigu.spark.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 9:18
 */
object PartitionBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List("hello", "hello", "world", "atguigu", "hello")
        
        val rdd1: RDD[(String, Int)] = sc.parallelize(list1, 2).map((_, 1))
        // 对 rdd进行重新分区
        val rdd2 = rdd1
            .map {
                case (k, v) => (v, k)
            }
            .partitionBy(new HashPartitioner(3))
            .map {
                case (v, k) => (k, v)
            }
        
        rdd2.glom().map(_.toList).collect().foreach(println)
        
        println("hello".hashCode % 3) // 1  -2
        println("world".hashCode % 3) // 0
        println("atguigu".hashCode % 3) // 2  -1
        println("atguigu".hashCode)
        
        sc.stop()
        
    }
}
