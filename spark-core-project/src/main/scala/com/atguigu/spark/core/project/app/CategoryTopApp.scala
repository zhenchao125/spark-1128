package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.{CategroyCount, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/5/11 10:29
 *
 */
object CategoryTopApp {
    // 需求1的实现
    def calcCategoryTop10(sc: SparkContext, actionRDD: RDD[UserVisitAction]) = {
        // 1. 创建累加器对象
        val acc = new CategoryAcc
        // 2. 注册累加器
        sc.register(acc, "CategoryAcc")
        // 3. 遍历RDD, 进行累加
        actionRDD.foreach(action => acc.add(action))
        // 4. 进数据进行处理   top10
        val map: mutable.Map[String, (Long, Long, Long)] = acc.value
        
        /*val result = map
            .toList
            .sortBy(x => x._2)(Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse))
            .take(10)*/
        
        val categoryList = map.map {
            case (cid, (click, order, pay)) =>
                CategroyCount(cid, click, order, pay)
        }.toList
        
        val result = categoryList
            .sortBy(x => (-x.clickCount, -x.orderCount, -x.payCount))
            .take(10)
        
        // 5. 把结果写到外部存储(jdbc)  TODO
        
        
        println(result)
        
    }
}

/*
用累加器一次的性计算出来 点击量, 下单量, 支付量
 */