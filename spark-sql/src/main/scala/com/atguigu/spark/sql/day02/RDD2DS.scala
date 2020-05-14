package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/14 9:27
 */
object RDD2DS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RDD2DS")
            .getOrCreate()
        import spark.implicits._
        val list = User(10, "lisi") :: User(20, "zs") :: User(15, "ww") :: Nil
        val rdd = spark.sparkContext.parallelize(list)
        val ds = rdd.toDS()
        ds.show()
        
        spark.close()
        
        
    }
}
