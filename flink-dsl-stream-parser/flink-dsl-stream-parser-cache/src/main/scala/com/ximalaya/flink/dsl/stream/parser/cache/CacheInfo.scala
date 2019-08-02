package com.ximalaya.flink.dsl.stream.parser.cache

import java.util.concurrent.TimeUnit
import java.util.function.Function

import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.side.CastErrorStrategy
import com.ximalaya.flink.dsl.stream.parser._
/**
  *
  * @author martin.dong
  *
  **/

sealed trait CacheInfo
class HDFSCacheInfo(val cacheName:String,
                    val cacheKeyField:CacheField,
                    val cacheValueField:CacheField,
                    val keyPattern:String,
                    val keyDecoder:FieldDecoder,
                    val updatePeriod:Int,
                    val updateTimeUnit:TimeUnit,
                    val castError: CastErrorStrategy,
                    val optional:Map[String,String],
                    val dynamicDecoders:Map[String,FieldDecoder],
                    val dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CacheInfo {

  require(optional.contains("path") &&
    optional.contains("fsDefaultName") &&
    optional.contains("zip") &&
    optional.contains("dataType"))

  //hdfs parameters
  val path:String = optional("path").trim
  val fsDefaultName:String = optional("fsDefaultName").trim
  val zip:Function[Array[Byte], Array[Byte]] = optional("zip").trim.toUnzipStrategy
  val dataType:MessageDecoder[AnyRef] = optional("dataType").trim.toSourceDataType(dynamicSourceDataType)

  override def toString:String = s"HDFSCacheInfo(cacheName=$cacheName cacheKeyField=$cacheKeyField cacheValueField=$cacheValueField " +
    s"keyPattern=$keyPattern keyDecoder=$keyDecoder updatePeriod=$updatePeriod updateTimeUnit=$updateTimeUnit castError=$castError path=$path" +
    s"fsDefaultName=$fsDefaultName zip=$zip dataType=$dataType)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other:HDFSCacheInfo⇒
        this.cacheName == other.cacheName &&
          this.cacheKeyField == other.cacheKeyField &&
          this.cacheValueField == other.cacheValueField &&
          this.keyPattern == other.keyPattern &&
          this.keyDecoder == other.keyDecoder &&
          this.updatePeriod == other.updatePeriod &&
          this.updateTimeUnit == other.updateTimeUnit &&
          this.castError == other.castError &&
          this.path == other.path &&
          this.fsDefaultName == this.fsDefaultName &&
          this.zip == this.zip &&
          this.dataType == this.dataType
      case _ ⇒ false
    }
  }

}

class RedisCacheInfo(val cacheName:String,
                     val cacheKeyField:CacheField,
                     val cacheValueField:CacheField,
                     val keyPattern:String,
                     val keyDecoder:FieldDecoder,
                     val updatePeriod:Int,
                     val updateTimeUnit:TimeUnit,
                     val castError: CastErrorStrategy,
                     val optional:Map[String,String],
                     val dynamicDecoders:Map[String,FieldDecoder],
                     val dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CacheInfo{
}

class HBaseCacheInfo(val cacheName:String,
                     val cacheKeyField:CacheField,
                     val cacheValueField:CacheField,
                     val keyPattern:String,
                     val keyDecoder:FieldDecoder,
                     val updatePeriod:Int,
                     val updateTimeUnit:TimeUnit,
                     val castError: CastErrorStrategy,
                     val optional:Map[String,String],
                     val dynamicDecoders:Map[String,FieldDecoder],
                     val dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CacheInfo {
}

class MysqlCacheInfo(val cacheName:String,
                     val cacheKeyField:CacheField,
                     val cacheValueField:CacheField,
                     val keyPattern:String,
                     val keyDecoder:FieldDecoder,
                     val updatePeriod:Int,
                     val updateTimeUnit:TimeUnit,
                     val castError: CastErrorStrategy,
                     val optional:Map[String,String],
                     val dynamicDecoders:Map[String,FieldDecoder],
                     val dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CacheInfo{
}


object CacheInfo {

  val parameters:Array[Class[_]] = Array[Class[_]](
    classOf[String],
    classOf[CacheField],
    classOf[CacheField],
    classOf[String],
    classOf[FieldDecoder],
    classOf[Int],
    classOf[TimeUnit],
    classOf[CastErrorStrategy],
    classOf[Map[String,String]])


  def apply(cacheName: String,
            cacheKeyField: CacheField,
            cacheValueField: CacheField,
            cacheRequired: CacheRequired,
            optional: Map[String, String]): CacheInfo = {
    null
  }
}