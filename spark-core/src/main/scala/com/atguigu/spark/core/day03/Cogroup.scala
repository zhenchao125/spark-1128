package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 15:27
 */
object Cogroup {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        val rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc"), (2, "dd")))
        
        val rdd3 = rdd1
            .cogroup(rdd2)
            .flatMap {
                case (k, (leftIt, rightIt)) =>
                    // leftIt = CompactBuffer(a, b)
                    // rightIt = CompactBuffer(aa, bb)
                    leftIt.flatMap(x => rightIt.map(y => (k, (x, y))))
            }
        // 用cogroup实现join的功能  1 -> (a, aa)  1 ->(b, aa)  => {(a, aa), (b, aa)} => {}
        rdd3.collect.foreach(println)
        
    }
}
