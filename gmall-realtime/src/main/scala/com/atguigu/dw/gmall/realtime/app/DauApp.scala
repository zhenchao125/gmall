package com.atguigu.dw.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.dw.gmall.common.constant.GmallConstant
import com.atguigu.dw.gmall.common.util.MyESUtil
import com.atguigu.dw.gmall.realtime.bean.StartupLog
import com.atguigu.dw.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(5))
        val sourceStream: InputDStream[(String, String)] =
            MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)
        
        // 1. 调整数据结构
        val startupLogDSteam = sourceStream.map {
            case (_, log) => JSON.parseObject(log, classOf[StartupLog])
        }
        // 2. 对 startupLogDStream 做去重过滤
        val filteredDSteam: DStream[StartupLog] = startupLogDSteam.transform(rdd => {
            // a: 按照 uid 进行去重: 按照 uid 进行分组, 每组取一个
            val distinctRDD: RDD[StartupLog] = rdd.groupBy(_.uid).flatMap {
                case (_, it) => it.take(1)
            }
            distinctRDD.collect.foreach(println)
            // b: 从 redis 中读取清单过滤
            val client: Jedis = RedisUtil.getJedisClient
            val key = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            // 获取到 redis 清单, 每个周期获取一次
            val uids: util.Set[String] = client.smembers(key)
            // 必须把得到的 uids 进行广播, 否则在其他 Executor 上无法得到这个变量的值
            val uidsBD: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uids)
            // 返回过滤后的 RDD
            client.close()
            distinctRDD.filter(log => !uidsBD.value.contains(log.uid))
        })
        
        // 3. 保存到 redis.
        filteredDSteam.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                val startupLogs: List[StartupLog] = it.toList
                startupLogs.foreach(startupLog => {
                    // 存入到 Redis value 类型 set, 存储 uid
                    val key = "dau:" + startupLog.logDate
                    client.sadd(key, startupLog.uid)
                })
                client.close()
                // 4. 保存到 ES
                MyESUtil.insertBulk(GmallConstant.ES_INDEX_DAU, startupLogs)
            })
        })
        ssc.start()
        ssc.awaitTermination()
    }
}

/*

1. 把用户第一次访问的记录记下来

2. 当天该用户后面的访问过滤

 */