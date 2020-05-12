package com.atguigu.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/12 16:23
 */
object RDD2DF_2 {
    def main(args: Array[String]): Unit = {
        // 1. 首先需要创建 SparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("RDD2DF")
            .getOrCreate()
        import spark.implicits._
        val list = User(10, "lisi")::User(20, "zs")::User(15, "ww")::Nil
        val rdd: RDD[User] = spark.sparkContext.parallelize(list)
        
        val df: DataFrame = rdd.toDF
        df.show
        
        // 3. 关闭
        spark.close()
    }
}
case class User(age: Int, name: String)
/*
RDD中很多时候会封装一些数据, 封装样例类中


 */