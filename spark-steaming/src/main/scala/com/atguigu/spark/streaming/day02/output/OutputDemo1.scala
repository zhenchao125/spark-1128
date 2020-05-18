package com.atguigu.spark.streaming.day02.output

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 13:56
 */
object OutputDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OutputDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
    
        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        lineStream.saveAsTextFiles("word", "log")
        ssc.start()
        ssc.awaitTermination()
        
    }
}
