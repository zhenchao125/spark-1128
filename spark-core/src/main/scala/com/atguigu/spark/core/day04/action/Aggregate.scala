package com.atguigu.spark.core.day04.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * Author atguigu
 * Date 2020/5/8 10:24
 */
object Aggregate {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Aggregate").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val result = rdd1.aggregate(Int.MinValue)(
            (max, ele) => max.max(ele),
            (max1, max2) => max1 + max2   // 计算和的, 零值也会参与一次
        ) - Int.MinValue*/
    
        val list1 = List("a", "b", "c", "d", "e", "f")
        val rdd1: RDD[String] = sc.parallelize(list1, 3)
        
        /*val result = rdd1.aggregate("?")(_ + _, _ + _)
        */
        
        sc.stop()
        
    }
}
