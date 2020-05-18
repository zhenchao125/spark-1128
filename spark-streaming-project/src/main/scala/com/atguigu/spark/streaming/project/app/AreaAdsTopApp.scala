package com.atguigu.spark.streaming.project.app

import com.atguigu.spark.streaming.project.bean.AdsInfo
import com.atguigu.spark.streaming.project.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 15:32
 */
object AreaAdsTopApp {
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AreaAdsTopApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val adsLongStream = MyKafkaUtil
            .getKafkaStream(ssc, "ads_log1128")
            .map(log => {
                val splits: Array[String] = log.split(",")
                AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
            })
        adsLongStream.print(1000)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
