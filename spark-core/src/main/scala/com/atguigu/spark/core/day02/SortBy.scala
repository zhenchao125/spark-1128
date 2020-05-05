package com.atguigu.spark.core.day02

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * Author atguigu
 * Date 2020/5/5 15:32
 */
object SortBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        //        val list1 = List(30, 50, 70, 60, 10, 20)
        val list1 = List("aa", "cc", "abc", "hello", "b", "a", "c")
        val rdd1 = sc.parallelize(list1, 3)
        
        // 长度升, 相等后之后再按照字母表降
        val rdd2 = rdd1.sortBy(x => (x.length, x), true)(
            Ordering.Tuple2(Ordering.Int, Ordering.String.reverse), ClassTag(classOf[(Int, String)]))
        println(rdd2.getNumPartitions)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}

/*
sortBy
    整个RDD进行的全局排序
    参数1: 排序指标
    参数2:是否升序(默认true)
    参数3:排序后新的RDD的分区数.默认和排序前的rdd的分区数一致
 */