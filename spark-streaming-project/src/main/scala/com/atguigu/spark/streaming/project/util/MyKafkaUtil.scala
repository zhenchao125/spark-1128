package com.atguigu.spark.streaming.project.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Author atguigu
 * Date 2020/5/18 15:33
 */
object MyKafkaUtil {
    val params = Map[String, String](
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "group.id" -> "atguigu"
    )
    
    def getKafkaStream(ssc: StreamingContext, topic: String, otherTopics: String*) = {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            (otherTopics :+ topic).toSet
        ).map(_._2)
    }
}
