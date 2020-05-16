package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/16 16:25
 */
object DirectApi {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DirectApi")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val param = Map[String, String](
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            "group.id" -> "atguigu"
        )
        val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            param,
            Set("spark1128"))
        
        stream
            .map(_._2)
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
