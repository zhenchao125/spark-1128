package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 14:23
 */
object Practice2 {
    def main(args: Array[String]): Unit = {
        // 读取文件, 进行wordCount的统计, 然后按照单词的数量降序排列 (用sortByKey)
        // 10分钟
        val conf: SparkConf = new SparkConf().setAppName("Practice2").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc
            .textFile("c:/1128.txt")
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .aggregateByKey(0)(_ + _, _ + _)
            .map {
                case (k, v) => (v, k)
            }
            .sortByKey(ascending = false)
            .map {
                case (v, k) => (k, v)
            }
            .collect
            .foreach(println)
        
        sc.stop()
        
    }
}
