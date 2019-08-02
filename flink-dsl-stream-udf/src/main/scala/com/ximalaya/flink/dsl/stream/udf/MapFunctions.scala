package com.ximalaya.flink.dsl.stream.udf

import org.apache.flink.table.functions.ScalarFunction
import java.util.{Map ⇒ JMap}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.collect.Maps
/**
  *
  * @author martin.dong
  *
  **/

object MapFunctions {

  class MapEmpty extends ScalarFunction{
    def eval(map:JMap[_,_]):Boolean={
        map.isEmpty
    }
  }


  class MapSize extends ScalarFunction{
    def eval(map:JMap[_,_]):Int={
      map.size()
    }
  }

  import scala.collection.convert.wrapAsScala._
  class JsonIntMapConvert extends ScalarFunction{
    def eval(json:String):JMap[String,Int]={
      val result:JMap[String,Int] = Maps.newHashMap()
      val jsonObject = JSON.parseObject(json)
      jsonObject.keys.toList.foreach(key⇒result.put(key,
        Int.unbox(jsonObject.getInteger(key))))
      result
    }

    def eval(map:JMap[String,Int]):String={
      val jSONObject = new JSONObject()
      map.toList.foreach{
        case (k,v)⇒ jSONObject.put(k,Int.box(v))
      }
      jSONObject.toJSONString
    }
  }

  class CsvIntMapConvert extends ScalarFunction{
//    def eval(csv:String,separator1:String,separator2:String):JMap[String,Int]={
//        csv.split(separator1)
//          .map(kv⇒kv.split(separator1))
//          .map(kv⇒(kv(0),kv(1).toInt))
//          .toMap
//    }
    def eval(map:JMap[String,Int],separator1:String,separator2:String):String={
        map.toList.map{
          case (k,v)⇒ s"$k$separator2$v"
        }.mkString(separator1)
    }
  }


  class MapToJson extends ScalarFunction{
    def eval(map:JMap[String,_]):String={
        new JSONObject().fluentPutAll(map).toJSONString
    }
  }
}
