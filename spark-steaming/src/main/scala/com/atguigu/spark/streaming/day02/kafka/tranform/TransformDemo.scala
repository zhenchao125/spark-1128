package com.atguigu.spark.streaming.day02.kafka.tranform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 10:10
 */
object TransformDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val lineStream = ssc.socketTextStream("hadoop102", 9999)
        // 把对流的操作, 转换成对RDD操作.
        val result = lineStream.transform(rdd => {
            rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        })
        result.print
        
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
