package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/6 10:35
 */
object AggregateByKey2 {
    def main(args: Array[String]): Unit = {
        // 取出每个分区相同key对应值的最大值和最小值，然后最大值和最大值相加, 最小值和最小值相加
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        /*// 分区内: 最大和最小   分区间: 最大的和和最小的和
       val rdd2 =  rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
            {
                case ((max, min), value) => (max.max(value), min.min(value))
            },
            {
                case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
            }
            
        )*/
        // 计算每个key的平均值!
        // 分区内: 求和 与key出现的个数  分区间: 和相加 与 个数相加
        val rdd2 = rdd
            .aggregateByKey((0, 0))(
                {
                    case ((sum, count), value) => (sum + value, count + 1)
                },
                {
                    case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
                }
            )
            /*.map {
                case (key, (sum, count)) => (key, sum.toDouble / count)
            }*/
            .mapValues {
                case (sum, count) => sum.toDouble / count
            }
        
        rdd2.collect.foreach(println)
    }
}
