package com.atguigu.spark.core.day02.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 16:22
 */
object DoubleVlaue1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DoubleVlaue1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        
        
        // 并集(分区数相加)
        //        val rdd3: RDD[Int] = rdd1 ++ rdd2
        //        val rdd3: RDD[Int] = rdd1.union(rdd2)
        
        //交集(默认情况: 分区数和前面的rdd中最大那个相等) 去重
        //                val rdd3 = rdd1.intersection(rdd2)
        //                println(rdd3.getNumPartitions)
        // 差集()
        //        val rdd3 = rdd1.subtract(rdd2)
        // 笛卡尔积  一般很少使用.
        //        val rdd3 = rdd1.cartesian(rdd2)
        
        val list1 = List(30, 50, 70, 60)
        val list2 = List(3, 5, 7, 6, 1, 2)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[Int] = sc.parallelize(list2, 2)
        // zip
        //        val rdd3 = rdd1.zip(rdd2)
        //        val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => {
        ////            it1.zip(it2)  // 是scala的集合的zip
        //            it1.zipAll(it2, 100, -1)
        //        })
        val rdd3: RDD[(Int, Long)] = rdd1.zipWithIndex()
        
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}

/*
zip 站在元素的角度来拉:
    1. 两个RDD的分区数必须相同
    2. 对应的分区必须拥有相同的元素数( 总的元素数相同)
 */