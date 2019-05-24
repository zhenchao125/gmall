package com.atguigu.dw.gmallpublisher.service

import java.util

import com.atguigu.dw.gmall.common.constant.GmallConstant
import com.atguigu.dw.gmall.common.util.MyESUtil
import io.searchbox.client.JestClient
import io.searchbox.core.search.aggregation.TermsAggregation
import io.searchbox.core.{Search, SearchResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import scala.collection.mutable

// 加此注解 将来可以给PublisherService做注入
@Service
class PublisherServiceImp extends PublisherService {
    // 自动注入
    @Autowired
    private var jestClient: JestClient = _
    
    /**
      * 获取指定日期的日活总数
      *
      * @param date 指定的日期: 格式 2019-05-15
      */
    override def getDauTotal(date: String): Long = {
        // 1. 定义查询 DSL
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "logDate": "$date"
               |        }
               |      }
               |    }
               |  }
               |}
             """.stripMargin
        // 2. 创建 Search 对象
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.ES_INDEX_DAU)
            .addType("_doc").build()
        // 3. 执行查询
        val result: SearchResult = jestClient.execute(search)
        // 4. 返回总数
        result.getTotal.toLong
    }
    
    /**
      * 获取指定日期日活的小时统计
      *
      * @param date
      * @return
      */
    override def getDauHour2countMap(date: String): Map[String, Long] = {
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "logDate": "$date"
               |        }
               |      }
               |    }
               |  }
               |  , "aggs": {
               |    "groupby_hour": {
               |      "terms": {
               |        "field": "logHour",
               |        "size": 24
               |      }
               |    }
               |  }
               |}
             """.stripMargin
        
        val search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.ES_INDEX_DAU)
            .addType("_doc")
            .build()
        val result: SearchResult = jestClient.execute(search)
        val buckets: util.List[TermsAggregation#Entry] = result.getAggregations.getTermsAggregation("groupby_hour").getBuckets
        
        val hour2countMap: mutable.Map[String, Long] = mutable.Map[String, Long]()
        for (i <- 0 until buckets.size) {
            val bucket: TermsAggregation#Entry = buckets.get(i)
            hour2countMap += bucket.getKey -> bucket.getCount
        }
        hour2countMap.toMap
    }
    
    /**
      * 获取指定日期订单的销售额
      *
      * @param day
      * @return
      */
    override def getOrderTotalAmount(day: String): Double = {
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "createDate": "$day"
               |        }
               |      }
               |    }
               |  }
               |  , "aggs": {
               |    "sum_totalAmount": {
               |      "sum": {
               |        "field": "totalAmount"
               |      }
               |
               |    }
               |  }
               |}
             """.stripMargin
        
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.ES_INDEX_ORDER)
            .addType("_doc").build()
        
        val result: SearchResult = jestClient.execute(search)
        result.getAggregations.getSumAggregation("sum_totalAmount").getSum
    }
    
    /**
      * 获取指定日期每个小时的销售额
      *
      * @param day
      * @return
      */
    override def getOrderHourTotalAmount(day: String): Map[String, Double] = {
        val searchDSL: String =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "createDate": "$day"
               |        }
               |      }
               |    }
               |  }
               |  , "aggs": {
               |    "groupby_createHour": {
               |      "terms": {
               |        "field": "createHour",
               |        "size": 24
               |      }
               |      , "aggs": {
               |        "sum_totalAmount": {
               |          "sum": {
               |            "field": "totalAmount"
               |          }
               |        }
               |      }
               |    }
               |  }
               |}
             """.stripMargin
        
        val search: Search = new Search.Builder(searchDSL)
            .addIndex(GmallConstant.ES_INDEX_ORDER)
            .addType("_doc")
            .build()
        val result: SearchResult = jestClient.execute(search)
        // 得到聚合后的结果
        val buckets: util.List[TermsAggregation#Entry] =
            result.getAggregations.getTermsAggregation("groupby_createHour").getBuckets
        val hour2TotalAmount: mutable.Map[String, Double] = mutable.Map[String, Double]()
        import scala.collection.JavaConversions._
        for (bucket <- buckets) {
            hour2TotalAmount += bucket.getKey -> bucket.getSumAggregation("sum_totalAmount").getSum
        }
        hour2TotalAmount.toMap
    }
    
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
      *         "total": 100,
      *         "stat" : [
      *         {
      *         // 年龄段比例
      *         },
      *         {
      *         // 男女比例
      *         }
      *         ],
      *         "detail": {
      *         // 明细
      *         }
      *         }
      */
    override def getSaleDetailAndAggResultByAggField(date: String,
                                                     keyword: String,
                                                     startPage: Int,
                                                     size: Int,
                                                     aggField: String,
                                                     aggSize: Int): Map[String, Any] = {
        System.out.println("date = [" + date + "], keyword = [" + keyword + "], startPage = [" + startPage + "], size = [" + size + "], aggField = [" + aggField + "], aggSize = [" + aggSize + "]");
        // 统计每个年龄购买情况
        val searchDSL =
            s"""
               |{
               |  "from": ${(startPage - 1) * size},
               |  "size": $size,
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "dt": "$date"
               |        }
               |      }
               |      , "must": [
               |        {"match": {
               |          "sku_name": {
               |            "query": "$keyword",
               |            "operator": "and"
               |          }
               |        }}
               |      ]
               |    }
               |  }
               |  , "aggs": {
               |    "groupby_$aggField": {
               |      "terms": {
               |        "field": "$aggField",
               |        "size": $aggSize
               |      }
               |    }
               |  }
               |}
         """.stripMargin
        val search: Search = new Search.Builder(searchDSL)
            .addIndex("gmall_sale_detail")
            .addType("_doc")
            .build()
        
        val client: JestClient = MyESUtil.getClient()
        val result: SearchResult = client.execute(search)
        // 1. 得到总数
        val total: Integer = result.getTotal
        // 2. 得到明细 (scala 集合)
        var detailList: List[Map[String, Any]] = List[Map[String, Any]]() // 存储明细
        val hits: util.List[SearchResult#Hit[util.HashMap[String, Any], Void]] = result.getHits(classOf[util.HashMap[String, Any]])
        import scala.collection.JavaConversions._
        for (hit <- hits) {
            val source: util.HashMap[String, Any] = hit.source
            source.toMap
            detailList = source.toMap :: detailList
        }
        // 3. 得到聚合结果
        var aggMap: Map[String, Long] = Map[String, Long]() // 存储聚合结果
        val buckets: util.List[TermsAggregation#Entry] = result.getAggregations.getTermsAggregation(s"groupby_$aggField").getBuckets
        for (bucket <- buckets) {
            aggMap += bucket.getKey -> bucket.getCount
        }
        
        // 返回最终结果
        Map("total" -> total, "aggMap" -> aggMap, "detail" -> detailList)
    }
}
