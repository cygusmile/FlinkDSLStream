package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.parser._
import org.parboiled2.{ParserInput, Rule0, Rule1, _}

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  **/

case class BatchRequired(source:String,keyDecoder:String,parallel:Int)

/**
  * 离线表解析
  * @param input
  */
class CreateBatchTableParser(val input:ParserInput) extends DslStreamParser {
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    ignoreCase("create") ~ mustWhiteSpace ~ ignoreCase("batch") ~ mustWhiteSpace ~ ignoreCase("table")
  }


  def hasKey:Rule1[Option[String]] = rule{
    mayWhiteSpace ~ "," ~ mayWhiteSpace ~ key ~ mayWhiteSpace ~> ((p:String) ⇒ push(Some(p)))
  }

  def nothing:Rule1[Option[String]] = rule{
    capture(MATCH) ~> ((_:String)⇒push(None))
  }

  def chooseK:Rule1[Option[String]] = rule{
    hasKey |  nothing
  }

  def required:Rule1[BatchRequired] = rule {
    "required" ~ mayWhiteSpace ~ '(' ~ mayWhiteSpace ~ "source" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~
      mayWhiteSpace ~','~
      mayWhiteSpace ~ "keyDecoder" ~ mayWhiteSpace ~ '=' ~ mayWhiteSpace ~ captureString ~ mayWhiteSpace ~ ','~
      mayWhiteSpace ~ "parallel" ~ mayWhiteSpace ~ "=" ~ mayWhiteSpace ~captureString ~ mayWhiteSpace ~')' ~> ((source:String,
                                                                                                                keyDecoder:String,
                                                                                                                parallel:String)⇒{
      push(BatchRequired(source.cleanString( true),
        keyDecoder.cleanString( true),
        parallel.cleanString( true).toInt))
    })
  }

  def inputLine = rule {
    mayWhiteSpace ~ action ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~  sourceFieldDefinition.+.separatedBy(mayWhiteSpace ~ ','~mayWhiteSpace) ~
      chooseK ~ ')' ~ mayWhiteSpace ~ required ~ mayWhiteSpace ~ optional ~ mayWhiteSpace ~ ignoreCase("as") ~ mayWhiteSpace ~
      capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~ mayWhiteSpace ~ EOI
  }
}

object CreateBatchTableParser {
  def apply(input: ParserInput,
            dynamicDecoders:Map[String,FieldDecoder],
            dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]): Either[Exception,CreateBatchTableInfo] = {
    val parser = new CreateBatchTableParser(input)
    val parseResult = parser.inputLine.run()
    parseResult match {
      case Success(value) ⇒
        val fields = value.head
        val primaryKey = value.tail.head
        val required = value.tail.tail.head
        val option = value.tail.tail.tail.head
        val tableName = value.tail.tail.tail.tail.head
        Right(CreateBatchTableInfo(tableName,fields,primaryKey,required,option,dynamicDecoders,dynamicSourceDataType))
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}