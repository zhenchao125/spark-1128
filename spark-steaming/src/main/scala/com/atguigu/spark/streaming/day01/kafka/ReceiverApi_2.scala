package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/16 15:24
 */
object ReceiverApi_2 {
    def createSSC(): StreamingContext = {
        println("createSSC.....")
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DirectApi")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck1")  // 对ssc做checkpoint.
    
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
        
        ssc
    }
    def main(args: Array[String]): Unit = {
        
        // 可以从checkpoint中恢复一个 StreamingContext, 保留着我们的状态
//        StreamingContext.getActiveOrCreate("./ck1", () => createSSC())
        val ssc = StreamingContext.getActiveOrCreate("./ck1", createSSC)
        ssc.start()
        ssc.awaitTermination()
        
    }
}
