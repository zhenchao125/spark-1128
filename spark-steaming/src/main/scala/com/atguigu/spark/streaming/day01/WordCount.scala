package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/16 10:22
 */
object WordCount {
    def main(args: Array[String]): Unit = {
        // 1. 先有上下文:  StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 从数据源读取数据, 得到 DStream  (RDD, DataSet, DataFrame)
        // 2.1 从socket读数据
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        // 3. 对DStream 做各种转换, 得到自己想要数据
        val resultStream = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        resultStream.print(100)
        // 4. 启动流
        ssc.start()
        // 5. 阻止程序退出
        ssc.awaitTermination()
    }
}
