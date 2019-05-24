package com.atguigu.dw.gmallpublisher.bean

import org.json4s.JValue

/**
  * 封装饼图中的数据
  * 代表一张饼图
  */
case class Stat(title: String, options: List[Opt])

/**
  * 表示结果中的一个选项
  * 男  10% ...
  * *
  * 20-20岁  30%
  */
case class Opt(name: String, value: String)

/**
  * 封装返回给前端的所有数据
  */
case class SaleInfo(total: Int, stats: List[Stat], detail: List[Map[String, Any]])