package com.atguigu.dw.gmallpublisher

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019/5/23 9:51 AM
  */
object Test {
    def main(args: Array[String]): Unit = {
        import scala.collection.JavaConversions._
        var map1 = new java.util.HashMap[String, String]()
        map1 += "1" -> "11"
        println(map1)
        println(map1.toMap)
    
    
        import org.json4s.jackson.Serialization.write
        implicit val formats = DefaultFormats
        println(write(User("lisi", 20)))
        
    }
}

case class User(name: String, age: Int)
