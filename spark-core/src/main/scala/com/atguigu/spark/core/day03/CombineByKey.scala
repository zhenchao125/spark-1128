package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 11:46
 */
object CombineByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        // 计算每个key的和 和他们的个数
        val rdd2 = rdd
            .combineByKey(
                (v: Int) => (v, 1), // v表示和  1表示个数
                (sumCount: (Int, Int), value: Int) =>
                    (sumCount._1 + value, sumCount._2 + 1),
                (sumCount1: (Int, Int), sumCount2: (Int, Int)) =>
                    (sumCount1._1 + sumCount2._1, sumCount1._2 + sumCount2._2)
            )
        rdd2.collect.foreach(println)
        
    }
}
