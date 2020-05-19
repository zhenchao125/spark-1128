package com.atguigu.spark.streaming.project.app

import com.atguigu.spark.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Author atguigu
 * Date 2020/5/18 15:32
 */
object AreaAdsTopApp extends App {
    override def doSomething(ssc: StreamingContext, adsInfoStream: DStream[AdsInfo]): Unit = {
        // 需求分析:
        /*
        
            DStream[(day, area, ads), 1]  updateStateByKey
            DStream[(day, area, ads), count]
            
            分组, top 3
            DStream[(day, area), (ads, count)]
            DStream[(day, area), it[(ads, count)]]
            排序取30

         */
        adsInfoStream
            .map(info => ((info.dayString, info.area, info.adsId), 1))
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
            .map {
                case ((day, area, ads), count) =>
                    ((day, area), (ads, count))
            }
            .groupByKey
            .mapValues(it => {
                it.toList.sortBy(-_._2).take(3)
            })
            .print
    }
}

/*
要么实时, 要么离线(批处理)
实时:
    难度小, 指标也少
离线:
    难度大, 指标也多
    
    
每天每地区热门广告 Top3

DStream[(day, area, ads), 1]  updateStateByKey
DStream[(day, area, ads), count]

分组, top 3
DStream[(day, area), (ads, count)]
DStream[(day, area), it[(ads, count)]]
排序取30




 */
