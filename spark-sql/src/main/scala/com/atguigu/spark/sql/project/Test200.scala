package com.atguigu.spark.sql.project

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/15 16:39
 */
object Test200 {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .appName("Test200")
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        
        spark.sql("use spark1128")
//        val df = spark.sql("select * from city_info")
        val df = spark.sql("select count(*) from city_info group by city_id")
        println(df.rdd.getNumPartitions)
        df.show
        spark.close()
        
        
    }
}
