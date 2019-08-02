package com.ximalaya.flink.dsl.stream.parser.create

import java.util.concurrent.TimeUnit

import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.side._
import com.ximalaya.flink.dsl.stream.parser._
import org.parboiled2.{Rule1, _}
import scala.util.{Failure, Success}
/**
  *
  * @author martin.dong
  *
  *
  **/
case class SideRequired( sourceType:String, keyEncoder:String, castError: CastErrorStrategy)
/**
  * 维表解析
  * @param input
  */
class CreateSideTableParser(val input:ParserInput) extends DslStreamParser {

  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    ignoreCase("create") ~ mustWhiteSpace ~ ignoreCase("side") ~ mustWhiteSpace ~ ignoreCase("table")
  }

  def none:Rule1[CacheStrategy] = rule{
     capture("none") ~> ((_:String) ⇒ push(Nothing))
  }

  def all:Rule1[CacheStrategy] = rule{
    "all" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ capture(string) ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~
       capture(CharPredicate.Digit.+) ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ capture(str("ms") | 's' | 'm' | 'h' | 'd' ) ~ mayWhiteSpace ~ ')' ~>(
      (pattern:String,ttl:String,timeUnit:String) ⇒ push(All(pattern.cleanString(true),ttl.toLong,timeUnit match {
        case "ms" ⇒ TimeUnit.MILLISECONDS
        case "s" ⇒ TimeUnit.SECONDS
        case "m" ⇒ TimeUnit.MINUTES
        case "h" ⇒ TimeUnit.HOURS
        case "d" ⇒ TimeUnit.DAYS
      }))
    )
  }

  def lru:Rule1[CacheStrategy] = rule{
    "lru" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ capture(string) ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ capture(CharPredicate.Digit.+) ~
      mayWhiteSpace ~ ',' ~mayWhiteSpace ~
      capture(CharPredicate.Digit.+) ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ capture(str("ms") | 's' | 'm' | 'h' | 'd' ) ~ mayWhiteSpace ~ ')' ~>(
      (pattern:String,cacheSize:String,ttl:String,timeUnit:String) ⇒ push(Lru(pattern.cleanString(true),cacheSize.toInt,ttl.toLong,timeUnit.toTimeUnit)))
  }

  def cacheStrategy:Rule1[CacheStrategy] = rule{
      none | all | lru
  }

  def cache:Rule1[CacheStrategy] = rule{
     "cache" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ cacheStrategy
  }

  def required:Rule1[SideRequired] = rule {
    "required" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ "source" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ capture('''~ mayWhiteSpace ~ source ~ mayWhiteSpace ~ ''') ~
      mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ "keyEncoder" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ',' ~
    mayWhiteSpace ~ "castError" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~  mayWhiteSpace ~ ')' ~> ((source: String,keyEncoder:String, castError: String) ⇒ {
      push(SideRequired(source.cleanString( true),
            keyEncoder.cleanString( true),
            castError.cleanString(true).toCastErrorStrategy))
    })
  }

  /**
    * 输入类型匹配规则
    * @return
    */
  def source:Rule0 = rule{
    ignoreCase("hbase") | ignoreCase("mysql") | ignoreCase("hdfs") | ignoreCase("redis")
  }

  def inputLine = rule {
    mayWhiteSpace ~ action ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~  sourceFieldDefinition.+.separatedBy(mayWhiteSpace ~ ','~mayWhiteSpace) ~
      mayWhiteSpace ~ ',' ~ mayWhiteSpace ~
      key ~ mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ cache ~ mayWhiteSpace ~ ')' ~ mayWhiteSpace ~ required ~ mayWhiteSpace ~ optional ~
      mayWhiteSpace ~ ignoreCase("as") ~ mayWhiteSpace ~ capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~ mayWhiteSpace ~ EOI
  }

}

object CreateSideTableParser{
  def apply(input: ParserInput,
            dynamicEncoders:Map[String,FieldEncoder],
            dynamicDecoders:Map[String,FieldDecoder]): Either[Exception,SideTableInfo] = {
      val parser = new CreateSideTableParser(input)
      val parseResult = parser.inputLine.run()
    parseResult match {
      case Success(value) ⇒
          val physicsSchema = value.head
          val primaryKey = value.tail.head
          val cacheStrategy = value.tail.tail.head
          val required = value.tail.tail.tail.head
          val sideProps = value.tail.tail.tail.tail.head
          val tableName = value.tail.tail.tail.tail.tail.head
          Right(CreateSideTableInfo(tableName,
            physicsSchema,
            primaryKey,
            cacheStrategy,
            required,sideProps,
            dynamicEncoders,
            dynamicDecoders))
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}
