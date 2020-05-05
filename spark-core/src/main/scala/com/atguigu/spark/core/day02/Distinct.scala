package com.atguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/5 14:25
 */
object Distinct {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        /*val list1 = List(30, 50, 70, 60, 10, 20, 60, 10, 20, 60, 10, 2, 10, 20, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        val rdd2: RDD[Int] = rdd1.distinct()  // 去重
        println(rdd2.collect().mkString(", "))*/
        
        val users = User(10, "zs")::User(20, "lisi")::User(10, "abc")::Nil
        val rdd1 = sc.parallelize(users, 2)
        val rdd2: RDD[User] = rdd1.distinct()
        rdd2.collect().foreach(println)
        sc.stop()
        
    }
}

case class User(age: Int, name: String){
    
    override def hashCode(): Int = this.age
    
    // 年龄相等就相等
    override def equals(obj: Any): Boolean = {
        obj match {
            case null => false
            case other: User => age == other.age
            case _ => false
        }
    }
}
