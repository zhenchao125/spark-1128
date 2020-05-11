package com.atguigu.spark.core.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/11 8:40
 */
object Demo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Demo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1= sc.parallelize(list1, 2).map((_, 1))
        
        rdd1.partitionBy(new RangePartitioner(3, rdd1)).collect
        
        
        sc.stop()
        
    }
}
