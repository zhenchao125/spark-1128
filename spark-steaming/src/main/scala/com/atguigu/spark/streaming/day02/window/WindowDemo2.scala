package com.atguigu.spark.streaming.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 11:14
 */
object WindowDemo2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck2")
        // 直接给DStream分配窗口, 将来所有的操作, 都是基于窗口
        val lineStream =
            ssc.socketTextStream("hadoop102", 9999).window(Seconds(9), Seconds(6))
        // 把对流的操作, 转换成对RDD操作.
        val result = lineStream
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        result.print
        
        ssc.start()
        ssc.awaitTermination()
    }
}
