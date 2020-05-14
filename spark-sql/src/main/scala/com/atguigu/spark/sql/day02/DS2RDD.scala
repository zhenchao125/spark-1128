package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/14 9:31
 */
object DS2RDD {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DS2RDD")
            .getOrCreate()
        import spark.implicits._
        val list = User(10, "lisi") :: User(20, "zs") :: User(15, "ww") :: Nil
        val rdd = spark.sparkContext.parallelize(list)
        val ds = rdd.toDS()
        
        // 转成rdd
        val rdd1 = ds.rdd
        rdd1.collect.foreach(println)
        
        spark.close()
        
        
    }
}
