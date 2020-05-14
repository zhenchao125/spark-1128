package com.atguigu.spark.sql.day02

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/14 10:31
 */
object RDD2DF_2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RDD2DF_2")
            .getOrCreate()
        
        // 1. 创建RDD[Row]
        val list = Array((10, "lisi"), (20, "zs"), (15, "ww"))
        var rdd = spark.sparkContext.parallelize(list).map{
            case (age, name) =>
                Row(age, name)
        }
        // 2. 给row的每个位置设置具体类型(schema)   设置列名和列的值的类型
        val schema = StructType(Array(StructField("age", IntegerType), StructField("name", StringType)))
        // 创建df
        val df = spark.createDataFrame(rdd, schema)
        df.show
        spark.close()
        
        
    }
}
