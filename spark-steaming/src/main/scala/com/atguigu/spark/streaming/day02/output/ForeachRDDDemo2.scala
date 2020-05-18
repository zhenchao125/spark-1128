package com.atguigu.spark.streaming.day02.output

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/18 13:56
 */
object ForeachRDDDemo2 {
    
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "aaaaaa"
    
    /*
    聚合的时候, 使用状态,
    写的时候, 如果数据存在, 则去更新
     */
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OutputDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck3")
        val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        val wordCountStream = lineStream.flatMap(_.split(" ")).map((_, 1))
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
        // 把wordCount数据写入到mysql中
        val spark = SparkSession.builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        import spark.implicits._
        wordCountStream.foreachRDD(rdd => {
            // 使用spark-sql来写
            // 1. 先创建sparkSession
            // 2. 把rdd转成df
            val df = rdd.toDF("word", "count")
            df.write.mode("overwrite").format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", pw)
                .option("dbtable", "word")
                .save()
            
        })
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
transform: 转换算子
foreachRDD: 行动

 */