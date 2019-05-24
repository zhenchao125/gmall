package com.atguigu.dw.gmallpublisher.service

trait PublisherService {
    /**
      * 获取指定日期的日活总数
      *
      * @param date 指定的日期: 格式 2019-05-15
      * @return 日活总数
      */
    def getDauTotal(date: String): Long
    
    /**
      * 获取指定日期日活的小时统计
      *
      * @param date
      * @return
      */
    def getDauHour2countMap(date: String): Map[String, Long]
    
    /**
      * 获取指定日期订单的销售额
      *
      * @param day
      * @return
      */
    def getOrderTotalAmount(day: String): Double
    
    /**
      * 获取指定日期每个小时的销售额
      *
      * @param day
      * @return
      */
    def getOrderHourTotalAmount(day: String): Map[String, Double]
    
    /**
      * 根据需要的聚合字段得到销售明细和聚合结构
      *
      * @param date      要查询的日期
      * @param keyword   要查询关键字
      * @param startPage 开始页面
      * @param size      每页显示多少条记录
      * @param aggField  要聚合的字段
      * @param aggSize   聚合后最多多少条记录
      * @return 1. 总数 2. 聚合结果 3. 明细
      *         {
      *             "total": 100,
      *             "stat" : [
      *                 {
      *                     // 年龄段比例
      *                 },
      *                 {
      *                     // 那女比例
      *                 }
      *             ],
      *             "detail": {
      *                 // 明细
      *             }
      *         }
      */
    def getSaleDetailAndAggResultByAggField(date: String,
                                            keyword: String,
                                            startPage: Int,
                                            size: Int,
                                            aggField: String,
                                            aggSize: Int): Map[String, Any]
}
