package com.atguigu.spark.streaming.project.app

import com.atguigu.spark.streaming.project.bean.AdsInfo
import com.atguigu.spark.streaming.project.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/19 9:11
 */
trait App {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AreaAdsTopApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck1")
    
        val adsInfoStream = MyKafkaUtil
            .getKafkaStream(ssc, "ads_log1128")
            .map(log => {
                val splits: Array[String] = log.split(",")
                AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
            })
        
        //不同的业务处理方法不一样
        doSomething(ssc, adsInfoStream)
        
        ssc.start()
        ssc.awaitTermination()
    
    }
    
    def doSomething(ssc: StreamingContext, stream: DStream[AdsInfo]):Unit
}
