package com.atguigu.spark.core.day03

import org.apache.spark.rdd.RDD
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
        
        
        sc.stop()
        
    }
}
