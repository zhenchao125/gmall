package com.atguigu.dw.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.dw.gmall.common.constant.GmallConstant
import com.atguigu.dw.gmall.common.util.MyESUtil
import com.atguigu.dw.gmall.realtime.bean.OrderInfo
import com.atguigu.dw.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/5/17 5:31 PM
  */
object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val ssc = new StreamingContext(conf, Seconds(2))
        val inputDSteam: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_ORDER)
        
        // 1. 类型的调整为 OrderInfo
        val orderInfoDStream: DStream[OrderInfo] = inputDSteam.map {
            case (_, jsonString) => {
                val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                // 对人名和电话号码脱敏
                orderInfo.consignee =
                    orderInfo.consignee.substring(0, 1) + "**"
                orderInfo.consigneeTel =
                    orderInfo.consigneeTel.substring(0, 3) + "****" + orderInfo.consigneeTel.substring(7, 11)
                orderInfo
            }
        }
        // 2. 开始向 ES 中写入数据
        orderInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition((orderInfoIt: Iterator[OrderInfo]) => {
                MyESUtil.insertBulk(GmallConstant.ES_INDEX_ORDER, orderInfoIt.toList)
            })
        })
        
        ssc.start()
        ssc.awaitTermination()
    }
}
