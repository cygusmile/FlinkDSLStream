package com.ximalaya.flink.dsl.stream.parser

import com.ximalaya.flink.dsl.stream.`type`.{Decoder, SourceDataType}
import com.ximalaya.flink.dsl.stream.api.message.decoder.{MessageDecoder, UnzipMessageDecoder}
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import org.parboiled2.{CharPredicate, ParserInput, Rule0, Rule1}

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class SourceDataTypeParser(val input:ParserInput,val dynamicDataTypes:Map[String,MessageDecoder[AnyRef]]) extends DslStreamParser {

  /**
    * 匹配单个标识符 并将路径名推入值栈
    * @return
    */
  def sourceDataType:Rule1[Option[MessageDecoder[AnyRef]]] = rule {
    capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~> ((identity:String)⇒{
      val buildInDecoder = Option.apply(SourceDataType.get(identity))
      buildInDecoder match {
        case Some(_)⇒push(buildInDecoder.map(_.asInstanceOf[MessageDecoder[AnyRef]]))
        case None⇒ push(dynamicDataTypes.get(identity))
      }
    })
  }

  def unzipType:Rule0 = rule{
    "snappy" | "gzip" | "deflate" | "lz4"
  }

  def unzipDecoder:Rule1[Option[MessageDecoder[AnyRef]]] = rule {
    "unzip" ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~ sourceDataType ~ mayWhiteSpace ~ "," ~ mayWhiteSpace ~ capture(unzipType) ~
      mayWhiteSpace ~ ")" ~> ((msd:Option[MessageDecoder[AnyRef]], t:String)⇒{
      msd match {
        case None⇒push(None)
        case Some(m) ⇒
          push(Option.apply(new UnzipMessageDecoder[AnyRef](m.asInstanceOf[MessageDecoder[AnyRef]],
            t.toUnzipStrategy)).map(_.asInstanceOf[MessageDecoder[AnyRef]]))

      }
    })
  }

  def inputLine:Rule1[Option[MessageDecoder[AnyRef]]] = rule {
    unzipDecoder | sourceDataType
  }
}

object SourceDataTypeParser{
  def apply(input: ParserInput,dynamicDataTypes:Map[String,MessageDecoder[AnyRef]]):Either[Exception,MessageDecoder[AnyRef]] = {
    val parser = new SourceDataTypeParser(input,dynamicDataTypes)
    val result = parser.inputLine.run()
    result match {
      case Success(value) ⇒ Right(value.get)
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}
