package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategroyCount, SessionInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/5/11 14:59
 */
object CategorySessionTopApp {
    def statCategoryTop10Session(sc: SparkContext,
                                 categoryCountList: List[CategroyCount],
                                 userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1. 过滤出来 top10品类的所有点击记录
        // 1.1 先map出来top10的品类id
        val cids = categoryCountList.map(_.cid.toLong)
        val topCategoryActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        // 2. 计算每个品类 下的每个session 的点击量  rdd ((cid, sid) ,1)
        val cidAndSidCount = topCategoryActionRDD
            .map(action => ((action.click_category_id, action.session_id), 1))
            .reduceByKey(_ + _)
            .map {
                case ((cid, sid), count) => (cid, (sid, count))
            }
        //   3. 按照品类分组,
        val cidAndSidCountGrouped: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
        // 4. 排序, 取top10
        val result = cidAndSidCountGrouped.map {
            case (cid, sidCountIt) =>
                // sidCountIt 排序, 取前10
                // Iterable转成容器式集合的时候, 如果数据量过大, 极有可能导致oom
                (cid, sidCountIt.toList.sortBy(-_._2).take(5))
        }
        
        result.collect.foreach(println)
    }
    
    def statCategoryTop10Session_1(sc: SparkContext,
                                   categoryCountList: List[CategroyCount],
                                   userVisitActionRDD: RDD[UserVisitAction]) = {
        
        val cids = categoryCountList.map(_.cid.toLong)
        val topCategoryActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        
        val cidAndSidCount: RDD[(Long, (String, Int))] = topCategoryActionRDD
            .map(action => ((action.click_category_id, action.session_id), 1))
            .reduceByKey(_ + _)
            .map {
                case ((cid, sid), count) => (cid, (sid, count))
            }
        
        // cid1 cid2
        // 5. 分别过滤出来没给品类的数据, 然后使用rdd的排序功能
        cidAndSidCount.cache()
        for (cid <- cids) {
            val arr = cidAndSidCount.filter(cid == _._1)
                .sortBy(-_._2._2)
                .take(5)
            println(arr.toList)
            
        }
    }
    
    def statCategoryTop10Session_2(sc: SparkContext,
                                   categoryCountList: List[CategroyCount],
                                   userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1. 过滤出来 top10品类的所有点击记录
        // 1.1 先map出来top10的品类id
        val cids = categoryCountList.map(_.cid.toLong)
        val topCategoryActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        // 2. 计算每个品类 下的每个session 的点击量  rdd ((cid, sid) ,1)
        val cidAndSidCount: RDD[(Long, (String, Int))] = topCategoryActionRDD
            .map(action => ((action.click_category_id, action.session_id), 1))
            .reduceByKey(_ + _)
            .map {
                case ((cid, sid), count) => (cid, (sid, count))
            }
        //   3. 按照品类分组,
        val cidAndSidCountGrouped: RDD[(Long, Iterable[(String, Int)])] = cidAndSidCount.groupByKey()
        // 4. 排序, 取top10
        val result = cidAndSidCountGrouped.map {
            case (cid, sidCountIt) =>
                // sidCountIt 要排序, 但是又不想转成容器式的集合? 怎么做?
                // 如果不转, 绝对不能用scala的sortBy !
                // 找一个可以自动排序的集合(TreeSet), 只需要让TreeSet集合的长度保持10就行了.
                var set: mutable.TreeSet[SessionInfo] = mutable.TreeSet[SessionInfo]()
                sidCountIt.foreach {
                    case (sid, count) =>
                        val info: SessionInfo = SessionInfo(sid, count)
                        set += info
                        if (set.size > 10) set = set.take(10)
                }
                (cid, set.toList)
        }
        
        result.collect.foreach(println)
        
        Thread.sleep(1000000)
    }
}
