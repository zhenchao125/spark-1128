package com.atguigu.spark.sql.day02.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Author atguigu
 * Date 2020/5/14 11:17
 */
object UDAFDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("UDAFDemo")
            .getOrCreate()
        spark.udf.register("my_sum", new MySum)
        val df = spark.read.json("c:/json/people.json")
        df.createOrReplaceTempView("p")
        spark.sql("select my_sum(age) from p").show
        spark.close()
        
        
    }
}

// UDAF
class MySum extends UserDefinedAggregateFunction {
    // 输入的数据的类型  StructType可以封装多个  Double
    override def inputSchema: StructType = StructType(StructField("ele", DoubleType) :: Nil)
    
    // 缓冲区的数据类型  Double
    override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: Nil)
    
    // 最终聚合结果的类型   Double
    override def dataType: DataType = DoubleType
    
    // 确定性. 相同的输入是否返回形同输出  相同的输入有相同输出
    override def deterministic: Boolean = true
    
    // 初始化: 对缓冲区输出化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 缓冲区, 可以看成一个集合, 当前只缓冲一个值  buffer(0)
        buffer(0) = 0D
    }
    
    // 分区内聚合 参数1: 缓冲区 参数: 传过来的每行数据 行的列数由传给聚合函数的参数来定
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 每个row中传一个值过来, 应该把这个值和缓冲区的值进行合并, 然后更新缓冲区
        // 有个bug  TODO
        if (!input.isNullAt(0)) {
            buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        }
    }
    
    // 分区间聚合 把buffer2中的数据, 聚合到buffer1中
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // 分区间的合并, 一定是每个分区已经聚合完成了.
        buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0) // 等价于: buffer1.update(0,  buffer1.getDouble(0) + buffer2.getDouble(0) )
    }
    
    // 返回最终的聚合结果
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}