PUT gmall2019_sale_detail
{
  "mappings": {
    "_doc":{
      "properties":{
         "user_id":{
           "type":"keyword"
         },
         "sku_id":{
           "type":"keyword"
         },
         "user_gender":{
           "type":"keyword"
         },
         "user_age":{
           "type":"short"
         },
         "user_level":{
           "type":"keyword"
         },
         "sku_price":{
           "type":"double" 
         },
         "sku_name":{
           "type":"text",
           "analyzer": "ik_max_word"
         },
         "sku_tm_id ":{
           "type":"keyword"
         },
         "sku_category3_id":{
           "type":"keyword"
         },
         "sku_category2_id":{
           "type":"keyword"
         },
         "sku_category1_id":{
           "type":"keyword"
         },
         "sku_category3_name":{
           "type":"text",
           "analyzer": "ik_max_word"
         },
         "sku_category2_name":{
           "type":"text",
           "analyzer": "ik_max_word"
         },
         "sku_category1_name":{
           "type":"text",
           "analyzer": "ik_max_word"
         },
         "spu_id":{
           "type":"keyword"
         },
         "sku_num":{
           "type":"long"
         },
         "order_count":{
           "type":"long"
         },
         "order_amount":{
           "type":"long"
         },
         "dt":{
           "type":"keyword"
         } 
      }
    }
  }
}
