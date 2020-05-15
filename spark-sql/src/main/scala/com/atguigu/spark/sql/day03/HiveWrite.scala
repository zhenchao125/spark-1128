package com.atguigu.spark.sql.day03

import org.apache.spark.sql.SparkSession

/**
 * Author atguigu
 * Date 2020/5/15 11:22
 */
object HiveWrite {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveWrite")
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        spark.sql("use spark1128")
        // 1. 先有df
//        val df = spark.read.json("c:/json/people.json")
        val df = List(("a", 11L), ("b", 22L)).toDF("n", "a")
//        df.printSchema()
        // 2. 写法1: 使用saveAsTable
        //        df.write.saveAsTable("user_1")
//        df.write.mode("append").saveAsTable("user_1")
        
        // 3. 写法2: 使用 insertInto
        //        df.write.insertInto("user_1")  // 大致等价于: df.write.mode("append").saveAsTable("user_1")
       // df.write.insertInto("user_1")  // 大致等价于: df.write.mode("append").saveAsTable("user_1")
        
        // 4. 写法3: 使用 hive的insert 语句
        
        spark.sql("insert into table user_1 values('zs', 100)").show
        
        spark.sql("select * from user_1").show
        
        
        spark.close()
        
        
    }
}

/*
读的时候不需要权限, 写的时候一般才需要权限.

saveAsTable
  在保存的时候, 看列名, 只要列名一致, 顺序不重要
insertInto
    不看列名, 只看顺序(类型):
 */