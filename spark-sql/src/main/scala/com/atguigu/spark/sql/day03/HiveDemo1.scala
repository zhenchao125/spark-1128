package com.atguigu.spark.sql.day03

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/15 10:36
 */
object HiveDemo1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveDemo1")
            // 支持hive
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        
        
//        spark.sql("show databases").show
        spark.sql("use gmall")
        spark.sql("select count(*) from ads_uv_count").show
        
        spark.close()
        
        
    }
}
/*
在linux的安装包, 默认是支持的hive.
但是在代码中, 默认不支持hive

 */