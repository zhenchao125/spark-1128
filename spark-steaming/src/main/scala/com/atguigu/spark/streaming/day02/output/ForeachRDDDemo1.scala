package com.atguigu.spark.streaming.day02.output

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 13:56
 */
object ForeachRDDDemo1 {
    
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "aaaaaa"
    val sql = "insert into word values(?, ?)"
    
    /*
    聚合的时候, 使用状态,
    写的时候, 如果数据存在, 则去更新
     */
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OutputDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        val wordCountStream = lineStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        // 把wordCount数据写入到mysql中
        wordCountStream.foreachRDD(rdd => {
            // 也是把流的操作转换成操作RDD.
            // 向外部存储写入数据
            // 在驱动中
            rdd.foreachPartition(it => {
                // 建立到mysql的连接
                Class.forName(driver)
                val conn = DriverManager.getConnection(url, user, pw)
                // 写数据
                it.foreach {
                    case (word, count) =>
                        val ps = conn.prepareStatement(sql)
                        ps.setString(1, word)
                        ps.setInt(2, count)
                        ps.execute()
                        // 关闭连接
                        ps.close()
                }
                // 关闭连接
                conn.close()
            })
        })
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
transform: 转换算子
foreachRDD: 行动

 */