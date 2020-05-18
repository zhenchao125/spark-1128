package com.atguigu.spark.streaming.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 11:14
 */
object WindowDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck2")
        val lineStream = ssc.socketTextStream("hadoop102", 9999)
        // 把对流的操作, 转换成对RDD操作.
        val result = lineStream
            .flatMap(_.split(" "))
            .map((_, 1))
            //            .reduceByKeyAndWindow(_ + _, Seconds(9), slideDuration = Seconds(6)) // 窗口长度是: 9 滑动步长: 3(默认和批次时间一样)
//            .reduceByKeyAndWindow(_ + _, (now, pre) => now - pre, Seconds(9))
            // now是新进入的批次的聚合结果, pre离开的批次的聚合结果
            .reduceByKeyAndWindow(_ + _, (now, pre) => now - pre, Seconds(9), filterFunc = _._2 > 0)
        
        result.print
        
        ssc.start()
        ssc.awaitTermination()
    }
}
