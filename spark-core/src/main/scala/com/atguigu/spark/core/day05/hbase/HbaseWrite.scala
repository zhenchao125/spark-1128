/*
package com.atguigu.spark.core.day05.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/9 13:41
 */
object HbaseWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val list = List(
            ("2100", "zs", "male", "10"),
            ("2101", "li", "female", "11"),
            ("2102", "ww", "male", "12"))
        val rdd1 = sc.parallelize(list)
        
        // 把数据写入到Hbase
        // rdd1做成kv形式
        val resultRDD = rdd1.map {
            case (rowKey, name, gender, age) =>
                val rk = new ImmutableBytesWritable()
                rk.set(Bytes.toBytes(rowKey))
                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gender"), Bytes.toBytes(gender))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(age))
                (rk, put)
        }
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104") // zookeeper配置
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")  // 输出表
    
        val job: Job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        resultRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
        
        sc.stop()
        
    }
}
*/
