package com.atguigu.spark.streaming.project.app

import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer

/**
 * Author atguigu
 * Date 2020/5/19 10:03
 */
object Test {
    def main(args: Array[String]): Unit = {
        // [{"09:25":75},{"09:26":86},{"09:27":76}]
        // {"09:25":75,"09:26":86,"09:27":76}
        val a = Iterable(("09:25",75), ("09:26",86), ("09:27",76)).toMap
        import org.json4s.DefaultFormats
        val hmCountString = Serialization.write(a)(DefaultFormats)
        println(hmCountString)
    }
}
