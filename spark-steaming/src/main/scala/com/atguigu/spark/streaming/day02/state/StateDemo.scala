package com.atguigu.spark.streaming.day02.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 10:24
 */
object StateDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck2")
        val lineStream = ssc.socketTextStream("hadoop102", 9999)
        // 把对流的操作, 转换成对RDD操作.
       val result =  lineStream
            .flatMap(_.split(" "))
            .map((_, 1))
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
        result.print
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}
