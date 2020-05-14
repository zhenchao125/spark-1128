package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/14 8:59
 */
object CreateDataSet {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("CreateDataSet")
            .getOrCreate()
        import spark.implicits._
        // 1. 如何创建DS
        // 1.1 从一个序列之间得到一个DS
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val ds = list1.toDS
        /*ds.createOrReplaceTempView("a")
        spark.sql("select * from a where value > 30").show*/
        ds.filter(_ > 30).show()
        ds.filter("value > 30").show   //dsl*/
        // 1.2 在ds中存储样例类(几乎都是这种情况)
        val list = User(10, "lisi") :: User(20, "zs") :: User(15, "ww") :: Nil
        val ds = list.toDS()
        /*ds.createOrReplaceTempView("user")
        spark.sql("select age from user").show*/
        val ds1 = ds.filter(_.age > 10)
        ds1.show
        
        
    }
}

case class User(age: Int, name: String)