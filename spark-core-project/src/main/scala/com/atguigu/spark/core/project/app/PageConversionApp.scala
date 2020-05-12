package com.atguigu.spark.core.project.app

import java.text.DecimalFormat

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/5/12 10:45
 */
object PageConversionApp {
    def statPageConversionRate(sc: SparkContext,
                               userVisitActionRDD: RDD[UserVisitAction],
                               pagesStr: String) = {
        // 1. 目标跳转页面切开
        val pages = pagesStr.split(",")
        //        val prePages = pages.take(pages.length - 1)
        //        val prePages = pages.init
        val prePages: Array[String] = pages.dropRight(1)
        // 1.1 目标跳转流 (想要的流)
        val postPages = pages.tail
        // "1->2"  "2->3"  ....
        val targetFlows = prePages
            .zip(postPages)
            .map(prePost => prePost._1 + "->" + prePost._2)
        
        // 2. 计算分母  prePages中所有页面的访问量
        val pageAndCount: collection.Map[Long, Long] = userVisitActionRDD
            .filter(action => prePages.contains(action.page_id.toString)) // 过滤出来需要计算访问量的页面
            .map(action => (action.page_id, 1))
            .countByKey()
        
        // 3. 计算分子
        // 3.1 按照sessionId分组, 每个组内再去计算他们的调整量
        val actionGroupedRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD
            /*.map(action => (action.session_id, action))
            .groupByKey()*/
            .groupBy(_.session_id)
        val pagesFlowRDD = actionGroupedRDD.flatMap {
            case (sid, actionIt) =>
                // 计算每个session下的跳转量
                // 1->2  2->3   4->5
                // 3.2 按照时间进行排序
                val actions = actionIt.toList.sortBy(_.action_time)
                // 3.3 计算跳转量   如果能有一个集合存储就是  "1->2",  "2->3", "1->5", ....
                val preActions = actions.init // 干掉最后一个
                val posActions = actions.tail // 干掉第一个
                // 3.4 结果中有各种转换流, 我们只需要 1->2 2->3 3->3
                val allFlows = preActions.zip(posActions).map {
                    case (pre, post) => s"${pre.page_id}->${post.page_id}"
                }
                // 4.5 过滤出来目标跳转流
                val allTargetFlows: List[String] = allFlows.filter(flow => targetFlows.contains(flow))
                allTargetFlows
        }
        // 3.2 聚合跳转流
        val pageFlowCount: Array[(String, Int)] = pagesFlowRDD.map((_, 1)).reduceByKey(_ + _).collect
    
        val f = new DecimalFormat(".00%")
        // 4. 计算跳转率  ("1->2", 1000)   找页面1的点击量 10000     10%
        val result = pageFlowCount.map {
            case (flow, count) =>
                // 4.1 1->2  找到页面1的点击量
                val page = flow.split("->")(0).toLong
                val denominator = pageAndCount(page)
                (flow, f.format(count.toDouble / denominator))
        }
        
        result.foreach(println)
        
    }
    
}

/*
        "1,2,3,4,5,6,7"
跳转率:


分母:
    页面的点击量   1-6的

分子:
    "1->2"的个数
    "1->3"的个数
        ...


-------
reduceByKey 转换
countByKey 行动
 */