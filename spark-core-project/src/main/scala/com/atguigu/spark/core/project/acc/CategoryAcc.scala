package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/*
累加器类型: UserVisitAction
最终的返回值:
    计算每个品类的 点击量, 下单量, 支付量
    Map[品类, (clickCount, orderCount, payCount)]

 */
class CategoryAcc extends AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]] {
    //    self => // 自身类型  self === this
    // 可变map使用 val就可以了
    private val map = mutable.Map[String, (Long, Long, Long)]()
    
    // 盘零.  判断集合是否为空
    override def isZero: Boolean = map.isEmpty
    
    // 复制类加器
    override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]] = {
        val acc = new CategoryAcc
        //java:  synchronized(锁){   }
        acc.map.synchronized {
            acc.map ++= this.map
        }
        acc
    }
    
    // 重置累加器
    override def reset(): Unit = {
        // 是可变累加器, 重置其实就是清空
        map.clear()
    }
    
    // 累加  (分区内聚合, 累加)
    override def add(v: UserVisitAction): Unit = {
        /*
        进来的用户行为, 有可能是 点击, 也可能是搜索(不处理), 下单, 支付
        
        Map[品类, (clickCount, orderCount, payCount)]
         */
        v match {
            // 判断是否点击
            case action if action.click_category_id != -1 =>
                // 点击的品类id
                val cid = action.click_category_id.toString
                // map中已经存储的cid的(点击量,下单量,支付量)
                val (click, order, pay) = map.getOrElse(cid, (0L, 0L, 0L))
                map += cid -> (click + 1L, order, pay)
            // 判断是否为下单
            case action if action.order_category_ids != "null" =>
                // "1,2,3"
                val cids: Array[String] = action.order_category_ids.split(",")
                cids.foreach(cid => {
                    val (click, order, pay) = map.getOrElse(cid, (0L, 0L, 0L))
                    map += cid -> (click, order + 1L, pay)
                })
            // 判断是否为支付
            case action if action.pay_category_ids != "null" =>
                val cids: Array[String] = action.pay_category_ids.split(",")
                cids.foreach(cid => {
                    val (click, order, pay) = map.getOrElse(cid, (0L, 0L, 0L))
                    map += cid -> (click, order, pay + 1L)
                })
            // 其他行为不做处理
            case _ =>
        }
    }
    
    // 分区间的合并
    override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[String, (Long, Long, Long)]]): Unit = {
        // 涉及到map的合并!!!  other中的map, 合并this中的map
        other match {
            case o: CategoryAcc =>
            // 第一种合并方式 foreach
            /*o.map.foreach {
                case (cid, (click, order, pay)) => // other的计数
                    // this中的计数
                    val (thisClick, thisOrder, thisPay) = map.getOrElse(cid, (0L, 0L, 0L))
                    this.map += cid -> (thisClick + click, thisOrder + order, thisPay + pay)
            }*/
            // 第二种合并方式 foldLeft  自始至终只有一个可变的map(每个分区内)
            o.map.foldLeft(this.map){
                case (map, (cid, (click, order, pay))) =>
                    val (thisClick, thisOrder, thisPay) = map.getOrElse(cid, (0L, 0L, 0L))
                    map += cid -> (thisClick + click, thisOrder + order, thisPay + pay)
                    map
            }
            
            case _ =>
                throw new UnsupportedOperationException
        }
        
    }
    
    // 最终的返回值
    override def value: mutable.Map[String, (Long, Long, Long)] = map
}