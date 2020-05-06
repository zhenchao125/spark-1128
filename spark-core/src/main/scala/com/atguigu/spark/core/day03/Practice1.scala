package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 10:39
 */
object Practice1 {
    def main(args: Array[String]): Unit = {
        // 使用reduceByKey计算出来每个key的最大值
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd =
            sc.parallelize(Array(("a",3), ("a",2), ("c",4), ("c", 1), ("b",3), ("c",6), ("c",8), ("a", 1)), 2)
        
//        val rdd2 = rdd.reduceByKey((x, y) => x.max(y))
//        val rdd2 = rdd.reduceByKey(math.max)
        val rdd2 = rdd.foldByKey(Int.MinValue)((x, y) => x.max(y))
        rdd2.collect.foreach(println)
    
    }
}
