package com.atguigu.spark.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/12 16:48
 */
object DF2RDD {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("DF2RDD")
            .getOrCreate()
        val df: DataFrame = spark.read.json("c:/json/people.json")
        df.printSchema()
        // 从df转成rdd之后, rdd内存储的用意是Row
        // Row当成一个集合, 表示一行数据, 弱类型(类型与jdbc中的ResultSet)
        val rdd: RDD[Row] = df.rdd
        
        val rdd2 = rdd.map(row => {
            /*val age = if(row.isNullAt(0)) -1 else row.getLong(0)
            val name = row.getString(1)*/
            
            /*val age = row.getAs[Long]("age")  // getLong
            val name = row.getAs[String]("name")*/
            
            val age = row.get(0)
            val name = row.get(1)
            
            (age, name)
        })
        rdd2.collect.foreach(println)
        
    }
}
