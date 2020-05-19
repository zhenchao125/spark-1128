package com.atguigu.spark.streaming.project.app

import java.sql.{DriverManager, PreparedStatement}

import com.atguigu.spark.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

/**
 * Author atguigu
 * Date 2020/5/19 9:10
 */
object LastHourApp extends App {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "aaaaaa"
    //
    
    
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
            .foreachRDD(rdd => {
                // 如果主键(广告id)存在就更新, 不存在就插入
                val sql = "insert into last_hour_app values(?, ?) on duplicate key update hm_count=?"
                rdd.foreachPartition((it: Iterator[(String, Iterable[(String, Int)])]) => {
                    // 每个广告写一行
                    // adsId  作为主键 保证不重复           1
                    //hmCount  存储分钟点击量的json字符串    '{"09:25": 75, "09:26": 86, "09:27": 76}'
                    Class.forName(driver)
                    val conn = DriverManager.getConnection(url, user, pw)
                    it.foreach{
                        case (adsId, it) =>
                            // 1. 先把Iterable转成json字符串  json4s(json for scala)
                            import org.json4s.DefaultFormats
                            val hmCountString = Serialization.write(it.toMap)(DefaultFormats)
                            val ps: PreparedStatement = conn.prepareStatement(sql)
                            ps.setInt(1, adsId.toInt)
                            ps.setString(2, hmCountString)
                            ps.setString(3, hmCountString)
                            ps.execute()
                            ps.close()
                    }
                    conn.close()
                })
            })
            
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
-----

(1,ArrayBuffer((09:25,75), (09:26,86), (09:27,76)))
(2,ArrayBuffer((09:27,80), (09:25,80), (09:26,85)))
mysql 表如何设计:
adsId  作为主键 保证不重复           1
hmCount  存储分钟点击量的json字符串    '{"09:25": 75, "09:26": 86, "09:27": 76}'
 */