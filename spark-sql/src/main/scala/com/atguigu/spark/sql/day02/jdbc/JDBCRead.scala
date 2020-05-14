package com.atguigu.spark.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/14 15:26
 */
object JDBCRead {
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pwd = "aaaaaa"
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCRead")
            .getOrCreate()
        import spark.implicits._
        
        // 通用的读
        /*val df = spark.read.format("jdbc")
            .option("url",url)
            .option("user", user)
            .option("password", pwd)
            .option("dbtable", "user")
            .load()*/
        
        // 专用的读
        val props = new Properties()
        props.setProperty("user",user)
        props.setProperty("password", pwd)
        val df = spark.read.jdbc(url, "user", props)
        
        df.show
        
        
        spark.close()
    }
}
/*
rdd:
    读jdbc
        new JdbcRDD(...)
    写jdbc
        rdd.foreachPartition(it => ....)
 */