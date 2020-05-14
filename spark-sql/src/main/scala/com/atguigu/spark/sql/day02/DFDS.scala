package com.atguigu.spark.sql.day02

import javax.swing.text.html.CSS
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/14 9:59
 */
object DFDS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DFDS")
            .getOrCreate()
        import spark.implicits._
        // df -> ds  (简单->复杂)
        val df = spark.read.json("c:/json/people.json")
        val ds = df.as[People]
    
        val df1: DataFrame = ds.toDF
        df1.show
        
        spark.close()
        
        
    }
}
case class People(name: String, age: Long)
