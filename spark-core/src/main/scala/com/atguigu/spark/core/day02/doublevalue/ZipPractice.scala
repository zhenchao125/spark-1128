package com.atguigu.spark.core.day02.doublevalue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/5/5 16:53
 */
object ZipPractice {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ZipPractice").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        
        
        // rdd:  "30->50" , "50->70",  "70->60", "60->10", "10->20"
        val rdd1 = sc.parallelize(list1.init)
        
        val rdd2 = sc.parallelize(list1.tail)
        val rdd3= rdd1.zip(rdd2).map{
            case (a, b) => s"${a}->${b}"
        }
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
