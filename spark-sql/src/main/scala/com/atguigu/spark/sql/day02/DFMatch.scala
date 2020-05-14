package com.atguigu.spark.sql.day02

import org.apache.spark.sql.{Row, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/14 10:18
 */
object DFMatch {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DFMatch")
            .getOrCreate()
        import spark.implicits._
        val df = spark.read.json("c:/json/people.json")
        /*val df1 = df.map(row => {
            row match {
                case Row(age: Long, name: String) =>
            age
        }
        }).toDF("age")*/
        val df1 = df.map {
            case Row(age: Long, name: String) =>
                age
        }
        df1.show
        
        
        spark.close()
        
        
    }
}
