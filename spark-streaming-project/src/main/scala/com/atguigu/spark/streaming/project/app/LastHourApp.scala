package com.atguigu.spark.streaming.project.app

import com.atguigu.spark.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/19 9:10
 */
object LastHourApp extends App {
    override def doSomething(ssc: StreamingContext, adsInfoStream: DStream[AdsInfo]): Unit = {
        // 只完成具体的业务
        adsInfoStream
            .window(Minutes(60), Seconds(6)) // 以后所有的操作都是基于窗口的
            .map(info => ((info.adsId, info.hmString), 1))
            .reduceByKey(_ + _)
            .map {
                case ((ads, hm), count) => (ads, (hm, count))
            }
            // 把每个广告的每分钟点击量放在一起
            .groupByKey
            .print(1000)
    }
}

/*
统计各广告最近 1 小时内的点击量趋势：
	各广告最近 1 小时内各分钟的点击量, 每6秒更新一次
用到窗口:
	窗口的长度: 1h
	窗口的步长: 6s

((ads, hm), 1)  做聚合
((ads, hm), count)  做聚合
 */