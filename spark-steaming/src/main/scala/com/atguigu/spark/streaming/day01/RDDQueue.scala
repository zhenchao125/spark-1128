package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/5/16 11:19
 */
object RDDQueue {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("RDDQueue")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val queue = mutable.Queue[RDD[Int]]()
        val stream = ssc.queueStream(queue,oneAtATime = false)
        val result = stream.reduce(_ + _)
        result.print
        
        ssc.start()
        
        while (true) {
//            println(queue.size)
            val rdd = ssc.sparkContext.parallelize(1 to 100)
            queue.enqueue(rdd)
            Thread.sleep(100)
        }
        ssc.awaitTermination()
    }
}
