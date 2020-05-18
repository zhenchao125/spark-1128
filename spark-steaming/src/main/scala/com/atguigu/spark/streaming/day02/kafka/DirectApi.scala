package com.atguigu.spark.streaming.day02.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 9:13
 */
object DirectApi {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("DirectApi")
        
        val ssc = new StreamingContext(conf, Seconds(3))
        val topics = Array("spark1128")
    
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            "key.deserializer" -> classOf[StringDeserializer],  // key的反序列化器
            "value.deserializer" -> classOf[StringDeserializer], // value的反序列化器
            "group.id" -> "atguigu",
            "auto.offset.reset" -> "latest",  // 每次从最新的位置开始读
            "enable.auto.commit" -> (true: java.lang.Boolean)  // 自动提交kafka的offset
        )
        val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            locationStrategy = LocationStrategies.PreferConsistent, // 平均分布
            Subscribe[String, String](topics, kafkaParams)
        )
        
        stream.map(_.value()).print
        
        ssc.start()
        ssc.awaitTermination()
    }
}
