package com.atguigu.spark.core.day05.jdbc

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/9 10:43
 */
object JdbcWrite {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "aaaaaa"
    
    def main(args: Array[String]): Unit = {
        // 把rdd的数据写入到mysql
        val conf: SparkConf = new SparkConf().setAppName("JdbcWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        // wordCount, 然后把wordCount的数据写入到奥mysql
        val wordCount = sc
            .textFile("c:/1128.txt")
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _, 3)
        
        // 只能手动去写
        val sql = "insert into word_count1128 values(?, ?)"
        /*wordCount.foreach {
            case (word, count) =>
                // 加载驱动
                Class.forName(driver)
                // 获取连接
                val conn = DriverManager.getConnection(url, user, pw)
                // 写数据
                val ps = conn.prepareStatement(sql)
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.execute()
                // 关闭连接
                ps.close()
                conn.close()
        }*/
        
        /*wordCount.foreachPartition( it => {
            // it就是存储的每个分区数据
            // 建立到mysql的连接
            Class.forName(driver)
            // 获取连接
            val conn = DriverManager.getConnection(url, user, pw)
            
            it.foreach{
                case (word, count) =>
                    val ps = conn.prepareStatement(sql)
                    ps.setString(1, word)
                    ps.setInt(2, count)
                    ps.execute()
                    ps.close()
            }
            conn.close()
        } )*/
        
        wordCount.foreachPartition(it => {
            // it就是存储的每个分区数据
            // 建立到mysql的连接
            Class.forName(driver)
            // 获取连接
            val conn = DriverManager.getConnection(url, user, pw)
            val ps = conn.prepareStatement(sql)
            var max = 0 // 最大批次
            it.foreach {
                case (word, count) =>
                    ps.setString(1, word)
                    ps.setInt(2, count)
                    ps.addBatch()
                    max += 1
                    if (max >= 10) {
                        ps.executeBatch()
                        max = 0
                    }
            }
            // 最后一次不到提交的上限,  做收尾
            ps.executeBatch()
            conn.close()
        })
        
        
        sc.stop()
        
    }
}
