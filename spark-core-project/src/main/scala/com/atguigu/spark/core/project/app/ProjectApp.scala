package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategroyCount, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/11 10:18
 */
object ProjectApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        // 1. 读数据
        val sourceRDD: RDD[String] = sc.textFile("c:/user_visit_action.txt")
        // 2. 封装到样例中
        val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
            val splits: Array[String] = line.split("_")
            UserVisitAction(
                splits(0),
                splits(1).toLong,
                splits(2),
                splits(3).toLong,
                splits(4),
                splits(5),
                splits(6).toLong,
                splits(7).toLong,
                splits(8),
                splits(9),
                splits(10),
                splits(11),
                splits(12).toLong)
        })
        // 3. 需求1的分析   返回值是top10的品类id
        val categoryCountList: List[CategroyCount] = CategoryTopApp.calcCategoryTop10(sc, userVisitActionRDD)
       // 4. 需求2的分析
        CategorySessionTopApp.statCategoryTop10Session_3(sc, categoryCountList, userVisitActionRDD)
        
        sc.stop()
    }
}
