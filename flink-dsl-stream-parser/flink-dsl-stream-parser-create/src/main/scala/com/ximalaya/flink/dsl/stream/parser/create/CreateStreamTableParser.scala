package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.side.CastErrorStrategy
import com.ximalaya.flink.dsl.stream.parser.DslStreamParser
import org.parboiled2.{Rule1, _}
import com.ximalaya.flink.dsl.stream.parser._

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

/**
  * 水位策略
  */
sealed trait WaterMarkStrategy

/**
  * 允许固定时间延迟的水位生成策略
  * @param columnName 列名
  * @param maxDelay 最大延迟
  */
class MaxOuterOfOrder(val columnName:String,val maxDelay:Long) extends WaterMarkStrategy{
  override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other:MaxOuterOfOrder ⇒ other.columnName == this.columnName && other.maxDelay == this.maxDelay
        case _ ⇒ false
      }
  }

  override def toString: String = {
      s"MaxOuterOfOrder(columnName=$columnName maxDelay=$maxDelay)"
  }
}

/**
  * 递增的水位生成策略 如果违反水位递增规则选择的策略
  * @param columnName 列名
  * @param noMonotony 如果违反水位递增规则选择的策略 失败或忽略
  */
class Ascending(val columnName:String,val noMonotony:Boolean) extends WaterMarkStrategy{
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other:Ascending ⇒ other.columnName == this.columnName && other.noMonotony == this.noMonotony
      case _ ⇒ false
    }
  }

  override def toString: String = {
    s"Ascending(columnName=$columnName noMonotony=$noMonotony)"
  }
}

/*
  基于摄入时间的水位生成策略
 */
object Ingestion extends WaterMarkStrategy{
  override def toString: String = "Ingestion"
}

sealed trait KeyAndWaterMark
object Empty extends KeyAndWaterMark
case class KeyOnly(key:String) extends KeyAndWaterMark
case class WaterMarkOnly(waterMarkStrategy: WaterMarkStrategy) extends KeyAndWaterMark
case class Both(primaryKey:String,waterMarkStrategy: WaterMarkStrategy) extends KeyAndWaterMark

case class StreamRequired(source:String,dataType:String,keyDecoder:String,parallel:Int,castError:CastErrorStrategy)

/**
  * 流表解析
  * @param input
  */
class CreateStreamTableParser(val input:ParserInput) extends DslStreamParser {

  def both:Rule1[KeyAndWaterMark] = rule{
    mayWhiteSpace ~ "," ~ mayWhiteSpace ~ key ~ mayWhiteSpace ~ "," ~ mayWhiteSpace ~ waterMarkStrategy ~ mayWhiteSpace ~> ((p:String,w:WaterMarkStrategy)⇒push(Both(p,w)))
  }

  def keyOnly:Rule1[KeyAndWaterMark] = rule{
    mayWhiteSpace ~ "," ~ mayWhiteSpace ~ key ~ mayWhiteSpace ~> ((p:String) ⇒ push(KeyOnly(p)))
  }

  def waterMarkStrategyOnly:Rule1[KeyAndWaterMark] = rule{
    mayWhiteSpace ~ "," ~ mayWhiteSpace ~ waterMarkStrategy ~ mayWhiteSpace ~> ((w:WaterMarkStrategy)⇒push(WaterMarkOnly(w)))
  }

  def nothing:Rule1[KeyAndWaterMark] = rule{
    capture(MATCH) ~> ((_:String)⇒push(Empty))
  }

  def chooseKW:Rule1[KeyAndWaterMark] = rule{
    both | keyOnly | waterMarkStrategyOnly | nothing
  }

  def inputLine = rule {
    mayWhiteSpace ~ action ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~  sourceFieldDefinition.+.separatedBy(mayWhiteSpace ~ ','~mayWhiteSpace) ~
      chooseKW ~ ')' ~ mayWhiteSpace ~ required ~ mayWhiteSpace ~ optional ~ mayWhiteSpace ~ ignoreCase("as") ~ mayWhiteSpace ~
      capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~ mayWhiteSpace ~ EOI
  }

  /**
    * parse action
    *
    * @return
    */
  override def action: Rule0 = rule{
      ignoreCase("create") ~ mustWhiteSpace ~ ignoreCase("stream") ~ mustWhiteSpace ~ ignoreCase("table")
  }


  def required:Rule1[StreamRequired] = rule {
     "required" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ "source" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ capture('''~mayWhiteSpace~source~mayWhiteSpace~''') ~
       mayWhiteSpace ~ ',' ~ mayWhiteSpace ~ "dataType" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~mayWhiteSpace ~','~
        mayWhiteSpace ~ "keyDecoder" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ',' ~
       mayWhiteSpace ~ "parallel" ~ mayWhiteSpace ~ "=" ~ mayWhiteSpace ~captureString ~ mayWhiteSpace ~',' ~ mayWhiteSpace ~ "castError" ~
      mayWhiteSpace ~ "=" ~ mayWhiteSpace ~ captureString~mayWhiteSpace~')' ~> ((source:String,dataType:String,keyDecoder:String,parallel:String,castError:String)⇒{
       push(StreamRequired(source.cleanString( true),
            dataType.cleanString( true),
            keyDecoder.cleanString( true),
            parallel.cleanString( true).toInt,
            CastErrorStrategy(castError.cleanString( true))))
    })
  }

  /**
    * 输入类型匹配规则
    * @return
    */
  def source:Rule0 = rule{
    ignoreCase("kafka") | ignoreCase("hdfs") | ignoreCase("socket") | ignoreCase("rabbitmq")
  }


  /**
    * 基于摄入时间的水位策略
    * @return
    */
  def ingestion:Rule1[WaterMarkStrategy] = rule {
    capture("ingestion") ~> ((_:String)⇒push(Ingestion))
  }


  /**
    * 基于递增时间的水位策略
    * @return
    */
  def ascending:Rule1[WaterMarkStrategy] = rule {
    "ascending" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ alias ~
      mayWhiteSpace~','~ capture(bool) ~ mayWhiteSpace~')' ~> ((name:String,noMonotony:String)⇒{
      push(new Ascending(name,noMonotony.toBoolean))
    })
  }

  /**
    * 基于最大乱序时间的水位策略
    * @return
    */
  def maxOuterOfOrder:Rule1[WaterMarkStrategy] = rule {
    "maxOuterOfOder" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ alias ~
      mayWhiteSpace~','~ mayWhiteSpace ~ capture(predicate(CharPredicate.Digit).+) ~ mayWhiteSpace~')' ~> ((name:String, maxDelay:String)⇒{
      push(new MaxOuterOfOrder(name,maxDelay.toLong))
    })
  }

  /**
    * 可选的水位策略
    * @return
    */
  def chooseW:Rule1[WaterMarkStrategy]=rule{
    ingestion | ascending | maxOuterOfOrder
  }

  /**
    * 水位策略匹配规则 匹配出水位策略推入值栈
    * @return
    */
  def waterMarkStrategy:Rule1[WaterMarkStrategy]= rule{
    ignoreCase("watermark") ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ chooseW
  }
}

object CreateStreamTableParser {

  /**
    * 构建流表配置
    * @param input 创建流表的语句
    * @param dynamicDecoders 动态字段解析器
    * @param dynamicSourceDataType 动态事件解析器
    * @return
    */
  def apply(input: ParserInput,
            dynamicDecoders:Map[String,FieldDecoder],
            dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]): Either[Exception,CreateStreamTableInfo] = {
         val parser = new CreateStreamTableParser(input)
         val parseResult = parser.inputLine.run()
         parseResult match {
           case Success(value) ⇒
              val fields = value.head
              val primaryKeyAndWaterMark = value.tail.head
              val required = value.tail.tail.head
              val option = value.tail.tail.tail.head
              val tableName = value.tail.tail.tail.tail.head
              Right(CreateStreamTableInfo(tableName,fields,primaryKeyAndWaterMark,required,option,dynamicDecoders,dynamicSourceDataType))
           case Failure(throwable)⇒ parser.convertError(throwable)
         }
      }
}
