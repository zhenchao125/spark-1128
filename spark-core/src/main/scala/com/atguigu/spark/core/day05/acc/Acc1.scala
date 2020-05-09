package com.atguigu.spark.core.day05.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/9 15:06
 */
object Acc1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Acc1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        // 累加器 : 系统提供的累加器
        val acc: LongAccumulator = sc.longAccumulator
        
        /*// 行动算子
        rdd1.foreach(x => {
            acc.add(1)
        })*/
        
        rdd1.foreach(x => acc.add(x))
    
        println(acc.value)
        
        sc.stop()
        
    }
}
/*
计数器
spark: 累加器
 */