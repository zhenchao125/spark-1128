package com.atguigu.spark.sql.day02.jdbc

import java.util.Properties

import com.atguigu.spark.sql.day02.jdbc.JDBCRead.{pwd, user}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/14 16:01
 */
object JDBCWrite {
    
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pwd = "aaaaaa"
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCWrite")
            .getOrCreate()
        
        val df = spark.read.json("c:/json/people.json")
        
        // 通用的写
        /*df.write.format("jdbc")
            .options(Map(
                "url" -> url,
                "user" -> user,
                "password" -> pwd,
                "dbtable" -> "user1128"
            ))
//            .mode("append")
            .mode(SaveMode.Overwrite) // "overwrite"
            .save()*/
        
        // 专用的写
        val props = new Properties()
        props.setProperty("user",user)
        props.setProperty("password", pwd)
        df.write.jdbc(url, "user1128_1", props)
        
        spark.close()
        
        
    }
}
