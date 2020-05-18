/*
package com.atguigu.spark.streaming.day01.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/16 15:24
 */
object ReceiverApi {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiverApi")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        // (k, v)  k默认是 null, v 才是真正的数据
        // key的用处: 决定数据的分区. 如果key null, 轮询的分区
        val sourceStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
            ssc,
            "hadoop102:2181,hadoop103:2181,hadoop104:2181",
            "atguigu",
            Map("spark1128" -> 2))
        
        sourceStream
            .map(_._2)
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
*/
