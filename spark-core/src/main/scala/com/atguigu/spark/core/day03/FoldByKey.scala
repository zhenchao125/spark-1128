package com.atguigu.spark.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 10:18
 */
object FoldByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd =
            sc.parallelize(Array(("a",3), ("a",2), ("c",4), ("c", 1), ("b",3), ("c",6), ("c",8), ("a", 1)), 2)
    
        val rdd2: RDD[(String, Int)] = rdd.foldByKey(1)(_ + _)
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}
