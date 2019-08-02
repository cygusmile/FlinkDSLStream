package com.ximalaya.fink.dsl.stream.parser.save

import com.ximalaya.flink.dsl.stream.api.message.encoder.MessageEncoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.parser.DslStreamParser
import org.parboiled2.{CharPredicate, ParserInput, _}
import com.ximalaya.flink.dsl.stream.parser._

import scala.util.{Failure, Success}
/**
  *
  * @author martin.dong
  *
  **/
sealed trait CacheStrategy
object Nothing extends CacheStrategy
case class Cache(size:Int) extends CacheStrategy{
  override def toString: String = s"Cache(size=$size)"
}


sealed trait KeyAndCache
case class CacheOnly(cache: CacheStrategy) extends KeyAndCache
case class Both(key:String,cacheStrategy: CacheStrategy) extends KeyAndCache

case class SaveRequired(sink:String,keyEncoder:String,parallel:Int)
/**
  * 保存表
  * @param input
  */

class SaveParser(val input:ParserInput) extends DslStreamParser{
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    ignoreCase("save")
  }

  /**
    * 对required字段里的信息进行解析
    * @return
    */
  def required: Rule1[SaveRequired] = rule {
    "required" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ "sink" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~
      mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ "keyEncoder" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ',' ~
      mayWhiteSpace ~ "parallel" ~ mayWhiteSpace ~ "=" ~ mayWhiteSpace ~captureString ~ mayWhiteSpace ~')' ~> ((sink:String,
                                                                                                                keyEncoder:String, parallel:String)⇒{
      push(SaveRequired(sink.cleanString(true),
        keyEncoder.cleanString(true),
        parallel.cleanString(true).toInt))
    })
  }

  def no:Rule1[CacheStrategy] = rule{
      capture("none") ~> ((_:String)⇒push(Nothing))
  }

  def hasCache:Rule1[CacheStrategy] = rule{
     "(" ~ mayWhiteSpace ~ capture(CharPredicate.Digit.+) ~ mayWhiteSpace ~ ")" ~>
        ((size:String)⇒push(Cache(size.toInt)))
  }

  def cacheStrategy:Rule1[CacheStrategy] = rule{
      no | hasCache
  }

  def cache:Rule1[CacheStrategy] = rule{
    "cache" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ cacheStrategy
  }

  def both:Rule1[KeyAndCache] = rule{
     mayWhiteSpace ~ "," ~ mayWhiteSpace ~ cache ~ mayWhiteSpace ~ "," ~ mayWhiteSpace ~ key ~ mayWhiteSpace ~> ((c:CacheStrategy,k:String)⇒push(Both(k,c)))
  }


  def cacheOnly:Rule1[KeyAndCache] = rule{
    mayWhiteSpace ~ "," ~ mayWhiteSpace ~ cache ~ mayWhiteSpace ~> ((c:CacheStrategy)⇒push(CacheOnly(c)))
  }

  def chooseKC:Rule1[KeyAndCache] = rule{
    both | cacheOnly
  }

  def inputLine = rule {
    mayWhiteSpace ~ action ~ mayWhiteSpace ~ capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~ mayWhiteSpace ~
      "(" ~ mayWhiteSpace ~  sinkFieldDefinition.+.separatedBy(mayWhiteSpace ~ ','~mayWhiteSpace) ~
        chooseKC ~ ')' ~ mayWhiteSpace ~ required ~ mayWhiteSpace ~ optional ~ mayWhiteSpace ~ EOI
  }
}

object SaveParser{
  def apply(input: ParserInput,
            dynamicEncoders:Map[String,FieldEncoder],
            dynamicSinkDataType:Map[String,MessageEncoder]): Either[Exception,SaveTableInfo] = {
    val parser = new SaveParser(input)
    val parseResult = parser.inputLine.run()
    parseResult match {
      case Success(value) ⇒
        val tableName = value.head
        val fields = value.tail.head
        val keyAndCache = value.tail.tail.head
        val required = value.tail.tail.tail.head
        val option = value.tail.tail.tail.tail.head
        Right(SaveTableInfo(tableName,fields,keyAndCache,required,
          option,dynamicEncoders,dynamicSinkDataType))
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}
