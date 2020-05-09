/*
package com.atguigu.spark.core.day05.hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

/**
 * Author atguigu
 * Date 2020/5/9 13:41
 */
object HbaseRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104") // zookeeper配置
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
        
        // 通用的读法 noSql key-value cf
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat], // InputFormat
            classOf[ImmutableBytesWritable], //hbase + mapreduce
            classOf[Result]
        )
        
        val rdd2 = hbaseRDD.map {
            case (ibw, result) =>
//                Bytes.toString(ibw.get())
                // 把每一行所有的列都读出来, 然后放在一个map中, 组成一个json字符串
                var map = Map[String, String]()
                // 先把row放进去
                map += "rowKey" -> Bytes.toString(ibw.get())
                // 拿出来所有的列
                val cells: util.List[Cell] = result.listCells()
                // 导入里面的一些隐式转换函数, 可以自动把java的集合转成scala的集合
                import scala.collection.JavaConversions._
                for(cell <- cells){ // for循环, 只支持scala的集合
                    val key = Bytes.toString(CellUtil.cloneQualifier(cell))
                    val value = Bytes.toString(CellUtil.cloneValue(cell))
                    map += key -> value
                }
                // 把map序列化成json字符串
                // json4s 专门为scala准备的json工具
                implicit val d: DefaultFormats =  org.json4s.DefaultFormats
                Serialization.write(map)
        }
//        rdd2.collect.foreach(println)
        rdd2.saveAsTextFile("./hbase")
        sc.stop()
        
    }
}

/*
foreachPartition
 */*/
