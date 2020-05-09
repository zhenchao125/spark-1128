package com.atguigu.spark.core.day05.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

object SumAccumulatorDemo{
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SumAccumulatorDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setCheckpointDir("./ck1")
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        val acc = new SumAccumulator
        // 必须要向SparkContext注册累加器
        sc.register(acc, "first")
        val rdd2 = rdd1.map(x => {
            acc.add(x)
            x
        })
        rdd2.checkpoint()
        rdd2.collect
        println(acc.value)
        Thread.sleep(10000000)
        sc.stop()
        
    }
}

/**
 * Author atguigu
 * Date 2020/5/9 15:21
 */
class SumAccumulator extends AccumulatorV2[Int, Int] {
    
    // 仅仅内部使用
    private var _sum = 0
    
    // 判零. 对缓冲区进行初始化判断
    override def isZero: Boolean = _sum == 0
    
    // 复制累加器. 返回一个新的累加器, 让当前累加器中缓冲区的值, 复制到新的累加器
    override def copy(): AccumulatorV2[Int, Int] = {
        val acc: SumAccumulator = new SumAccumulator
        acc._sum = this._sum
        acc
    }
    
    // 重置累加器. 把累加器中缓冲区的值重置为 "零"
    override def reset(): Unit = _sum = 0
    
    // 累加. 分区内累加
    override def add(v: Int): Unit = _sum += v
    
    // 累加 分区间的合并  其实就是把other中值合并到this的值中
    override def merge(other: AccumulatorV2[Int, Int]): Unit = {
        other match {
            case o: SumAccumulator =>
                _sum += o._sum
            case _ =>
        }
    }
    
    // 返回最后累加的值
    override def value: Int = _sum
}
