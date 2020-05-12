package com.atguigu.spark.sql.day01

import org.apache
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/12 16:23
 */
object RDD2DF {
    def main(args: Array[String]): Unit = {
        // 1. 首先需要创建 SparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("RDD2DF")
            .getOrCreate()
        import spark.implicits._
        // 2. 得到df, 使用df编程
        /*val df = spark.read.json("c:/json/people.json")
        df.createOrReplaceTempView("p")
        spark.sql("select * from p").show*/
        
        // 把rdd转成df
        val rdd = spark.sparkContext.parallelize(List((10, "a"), (20, "b"), (30, "c"), (40, "d")))
        val df: DataFrame = rdd.toDF("age", "name")
        df.printSchema()
        df.show
        // 3. 关闭
        spark.close()
    }
}
