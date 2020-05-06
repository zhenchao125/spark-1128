package com.atguigu.spark.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 14:48
 */
object Join {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc"), (2, "dd")))
        // 内连接
        //        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
        // 左外连接 左边都有, 右边没有的用 None 来替换
        //        val rdd3 = rdd1.leftOuterJoin(rdd2)
        // 右外连接 右边都有, 左边没有的用 None
        //        val rdd3 = rdd1.rightOuterJoin(rdd2)
        val rdd3 = rdd1.fullOuterJoin(rdd2)
    
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
