package com.atguigu.spark.core.day04.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/8 16:21
 */
object MyPartitionerDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyPartitionerDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 7, 60, 1, 20, null, null)
        val rdd1 = sc.parallelize(list1, 4).map((_, 1))
        val rdd2 = rdd1.partitionBy(new MyPartitioner(2))
        
        val rdd3 = rdd2.reduceByKey(new MyPartitioner(2), _ + _)
       
        rdd3.glom().map(_.toList).collect.foreach(println)
        
        Thread.sleep(100000000)
        sc.stop()
        
    }
}

class MyPartitioner(val num: Int) extends Partitioner {
    // 返回分完区之后的分区数
    override def numPartitions: Int = num
    
    // 写一个hash分区器
    override def getPartition(key: Any): Int = {
        key match {
            case null => 0
            case _ => key.hashCode().abs % numPartitions
        }
    }
    
   override def hashCode(): Int = num
    
    override def equals(obj: Any): Boolean = {
        obj match {
            case null => false
            case o: MyPartitioner => num == o.num
            case _ => false
        }
    }
}
