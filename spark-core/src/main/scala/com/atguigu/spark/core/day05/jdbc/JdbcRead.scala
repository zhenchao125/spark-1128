package com.atguigu.spark.core.day05.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/9 10:23
 */
object JdbcRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("JdbcRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val user = "root"
        val pw = "aaaaaa"
        val rdd = new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, user, pw)
                // 千万不要关闭连接
            },
            "select id, name from user where id >= ? and id <= ?",
            1,
            10,
            2,
            row => {
                (row.getInt("id"), row.getString("name"))
            }
        )
        
        rdd.collect.foreach(println)
        
        sc.stop()
        /*
        jdbc编程:
            加载启动
            class.forName(..)
            DiverManager.get...
            conn.prestat..
                ...
                pre.ex
                resultSet
         */
        
    }
}
