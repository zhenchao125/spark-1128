package com.atguigu.spark.streaming.project.bean

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
// 1589787737517,华南,深圳,104,4
case class AdsInfo(ts: Long,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String,
                   var dayString: String = null, // 2019-12-18
                   var hmString: String = null) { // 11:20
    
    val date = new Date(ts)
    dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
    hmString = new SimpleDateFormat("HH:mm").format(date)
}

