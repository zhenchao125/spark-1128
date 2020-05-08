package com.atguigu.spark.core.day04.transmitfun

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
        val sc = new SparkContext(conf)
        
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        // 1. 创建对象: 在driver
        val searcher = new Searcher("hello") // 查找包含hello子字符串的元素
        // 2. 调用对象的方法: 在driver
        val result: RDD[String] = searcher.getMatchedRDD3(rdd)
        
        result.collect.foreach(println)
    }
}

//需求: 在 RDD 中查找出来包含 query 子字符串的元素

// query 为需要查找的子字符串
class Searcher(val query: String) {
    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String): Boolean = {
        s.contains(query)
    }
    
    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) = {
        // 调用filter: 在driver
        // 传递进去了一个函数: isMatch, isMatch在什么地方执行: executor上执行
        rdd.filter(isMatch) //
    }
    
    def getMatchedRDD2(rdd: RDD[String]) = {
        // query是类的属性, 则对象也要序列化过去. 所以, 类也要实现序列化接口
        rdd.filter(x => x.contains(query)) //
    }
    
    def getMatchedRDD3(rdd: RDD[String]) = {
        // 其实是一个局部变量, 和当前这个对象有关系吗? 没有
        val q = query
        rdd.filter(x => x.contains(q)) //
    }
    
}

