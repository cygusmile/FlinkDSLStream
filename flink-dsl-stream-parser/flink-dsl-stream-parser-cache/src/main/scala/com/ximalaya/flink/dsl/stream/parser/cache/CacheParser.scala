package com.ximalaya.flink.dsl.stream.parser.cache

import com.ximalaya.flink.dsl.stream.`type`.FieldType
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.side.CastErrorStrategy
import com.ximalaya.flink.dsl.stream.parser.{DslStreamParser, cache, _}
import org.parboiled2._

import scala.concurrent.duration.TimeUnit
import scala.util.{Failure, Success}
/**
  *
  * @author martin.dong
  *
  **/
case class CacheField(paths:Option[List[String]],name:String,fieldType: FieldType)
case class CacheRequired(source:String,keyPattern:String,keyDecoder:String,updatePeriod:Int,updateTimeUnit:TimeUnit,castError: CastErrorStrategy)

class CacheParser(val input:ParserInput) extends DslStreamParser {
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    ignoreCase("create") ~ mustWhiteSpace ~ ignoreCase("cache")
  }

  def cacheKey:Rule1[CacheField] = rule{
      morePath ~ mustWhiteSpace ~ "as" ~ mustWhiteSpace ~ "key" ~ mustWhiteSpace ~ fieldType ~> ((paths:Seq[String],fieldType:String)⇒{
           val (p,name) = paths.splitPaths
           CacheField(p,name,fieldType.toFieldType)
      })
  }

  def cacheValue:Rule1[CacheField] = rule{
    morePath ~ mustWhiteSpace ~ "as" ~ mustWhiteSpace ~ "value" ~ mustWhiteSpace ~ fieldType ~> ((paths:Seq[String],fieldType:String)⇒{
      val (p,name) = paths.splitPaths
      CacheField(p,name,fieldType.toFieldType)
    })
  }


  /**
    * 输入类型匹配规则
    * @return
    */
  def source:Rule0 = rule{
    ignoreCase("hbase") | ignoreCase("mysql") | ignoreCase("hdfs") | ignoreCase("redis")
  }


  def required:Rule1[CacheRequired] = rule {
    "required" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ "source" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ capture('''~ mayWhiteSpace ~ source ~ mayWhiteSpace ~ ''') ~
      mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ "keyPattern" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ',' ~
      mayWhiteSpace ~ "keyDecoder" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~  mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ "updatePeriod" ~ mayWhiteSpace ~ '=' ~
      mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ',' ~  mayWhiteSpace ~ "updateTimeUnit" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~
      "castError" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ')' ~> ((source: String,keyPattern:String,keyDecoder:String, updatePeriod:String,updateTimeUnit:String,castError: String) ⇒ {
      push(cache.CacheRequired(source.cleanString( true),
        keyPattern.cleanString(true),
        keyDecoder.cleanString( true),
        updatePeriod.cleanString(true).toInt,
        updateTimeUnit.cleanString(true).toTimeUnit,
        CastErrorStrategy(castError.cleanString(true))))
    })
  }

  def inputLine = rule {
    mayWhiteSpace ~ action ~ mustWhiteSpace ~ captureString ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~ cacheKey ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ cacheValue ~ mayWhiteSpace ~ ")" ~ mayWhiteSpace ~ required ~ mayWhiteSpace ~ optional ~  mayWhiteSpace ~ EOI
  }
}

object CacheParser{
  def apply(input: ParserInput,
            dynamicDecoders:Map[String,FieldDecoder],
            dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]): Either[Exception,CacheInfo] = {
    val parser = new CacheParser(input)
    val parseResult = parser.inputLine.run()
    parseResult match {
      case Success(value)⇒
          val tableName = value.head
          val cacheKeyField = value.tail.head
          val cacheValueField = value.tail.tail.head
          val cacheRequired = value.tail.tail.tail.head
          val optional = value.tail.tail.tail.tail.head
          Right(CacheInfo(tableName,cacheKeyField,cacheValueField,cacheRequired,optional))
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}
