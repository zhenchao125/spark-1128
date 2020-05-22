import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author atguigu
 * Date 2020/5/22 14:18
 */
object MapJoin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(List("hello", "atguigu", "atguigu", "hello")).map((_, 1))
        val rdd2 = sc.parallelize(List("hello", "hello", "atguigu")).map((_, 1))
        
//        val rdd3 = rdd1.join(rdd2)
        // map端join: 把其中一个小的RDD广播出去, 然后对另外一个比较大的做map, 在map内完成join逻辑
        val arrBd = sc.broadcast(rdd2.collect())
        val rdd3 = rdd1.flatMap{
                    // (hello,(1,1))
                    //(hello,(1,1))
                    //(hello,(1,1))
                    //(hello,(1,1))
                    //(atguigu,(1,1))
                    //(atguigu,(1,1))
            case (key, value) =>
                val data = arrBd.value
                // key是hello
                data.filter(_._1 == key).map{
                    case (k, v) => (k, (value, v))
                }
                
            
        }
        
        rdd3.collect.foreach(println)
        
        
        
        sc.stop()
        
    }
}
