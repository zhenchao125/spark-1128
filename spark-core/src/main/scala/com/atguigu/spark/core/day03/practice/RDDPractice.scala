package com.atguigu.spark.core.day03.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 16:07
 */
object RDDPractice {
    def main(args: Array[String]): Unit = {
        /*
        => 元数据做map
        => RDD((pro, ads), 1)  reduceByKey
        => RDD((pro, ads), count)   map
        => RDD(pro -> (ads, count), ....)      groupByKey
        => RDD( pro1-> List(ads1->100, abs2->800, abs3->600, ....),  pro2 -> List(...) )  map: 排序,前3
        => RDD( pro1-> List(ads1->100, abs2->800, abs3->600),  pro2 -> List(...) )
         */
        val conf: SparkConf = new SparkConf().setAppName("RDDPractice").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        // 1. 读取原始数据
        val lineRDD: RDD[String] = sc.textFile("c:/agent.log")
        // 2. 调整成我需要格式  ((pro, ads), 1)
        val proAdsAndOneRDD = lineRDD.map(line => {
            val split = line.split(" ")
            ((split(1), split(4)), 1)
        })
        // 3. RDD((pro, ads), count)
        val proAdsAndCountRDD = proAdsAndOneRDD.reduceByKey(_ + _)
        // 4. RDD(pro -> (ads, count), ....)
        val proAndAdsCountRDD = proAdsAndCountRDD.map {
            case ((pro, ads), count) => (pro, (ads, count))
        }
        // 5. RDD( pro1-> List(ads1->100, abs2->800, abs3->600, ....),  pro2 -> List(...) )  map: 排序,前3
        val resultRDD = proAndAdsCountRDD
            .groupByKey()
            .map {
                case (pro, adsCountIt: Iterable[(String, Int)]) =>
                    (pro, adsCountIt.toList.sortBy(-_._2).take(3))
            }
            //            .sortByKey()
            .sortBy(_._1.toInt)
        resultRDD.collect.foreach(println)
        sc.stop()
        
        
    }
}

/*
需求
1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
        1516609143867 6 7 64 16
        1516609143869 9 4 75 18
        1516609143869 1 7 87 12
2.	需求: 统计出每一个省份广告被点击次数的 TOP3

-------------
倒推法来分析:
=> 元数据做map
=> RDD((pro, ads), 1)  reduceByKey
=> RDD((pro, ads), count)   map
=> RDD(pro -> (ads, count), ....)      groupByKey
=> RDD( pro1-> List(ads1->100, abs2->800, abs3->600, ....),  pro2 -> List(...) )  map: 排序,前3
=> RDD( pro1-> List(ads1->100, abs2->800, abs3->600),  pro2 -> List(...) )

 */