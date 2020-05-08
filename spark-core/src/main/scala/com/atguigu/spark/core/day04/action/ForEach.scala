package com.atguigu.spark.core.day04.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/5/8 9:31
 */
object ForEach {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ForEach").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        rdd1.foreach(x => {
            // 建立到mysql的连接
            
            // 写
            
            // 关闭连接
        })
        
        rdd1.foreachPartition(it => {
            //
        })
        
        sc.stop()
        
    }
}
/*
foreach
    是遍历RDD中的每个元素
 */