package com.atguigu.spark.sql.day02.udf

import java.text.DecimalFormat

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/14 11:17
 */
object UDAFDemo_2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("UDAFDemo")
            .getOrCreate()
        spark.udf.register("my_avg", new MyAvg)
        val df = spark.read.json("c:/json/people.json")
        df.createOrReplaceTempView("p")
        spark.sql("select my_avg(age) from p").show
        spark.close()
        
        
    }
}
class MyAvg extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("ele", DoubleType) :: Nil)
    
    // 平均值: 需要缓存和 和个数
    override def bufferSchema: StructType =
        StructType(StructField("sum", DoubleType):: StructField("count", LongType) :: Nil)
    
    override def dataType: DataType = StringType
    
    override def deterministic: Boolean = true
    
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0D  // 初始和
        buffer(1) = 0L // 初始个数
    }
    
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if(!input.isNullAt(0)){
            buffer(0) = buffer.getDouble(0) +  input.getDouble(0)
            buffer(1) = buffer.getLong(1) +  1L  // 聚合个数
        }
    }
    
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getDouble(0) +  buffer2.getDouble(0)
        buffer1(1) = buffer1.getLong(1) +  buffer2.getLong(1)
    }
    
    override def evaluate(buffer: Row): String =
        new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getLong(1))
}
