package com.ximalaya.flink.dsl.stream.parser

import com.ximalaya.flink.dsl.stream.`type`.Encoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.{FieldEncoder, ZipFieldEncoder}
import org.parboiled2.{CharPredicate, ParserInput, Rule0, Rule1}

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class EncoderParser(val input:ParserInput,val dynamicEncoders:Map[String,FieldEncoder]) extends DslStreamParser {

  /**
    * 匹配单个标识符 并将路径名推入值栈
    * @return
    */
  def encoder:Rule1[Option[FieldEncoder]] = rule {
    capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~> ((identity:String)⇒{
      val buildInEncoder = Option.apply(Encoder.get(identity))
      buildInEncoder match {
        case Some(_)⇒push(buildInEncoder)
        case None⇒ push(dynamicEncoders.get(identity))
      }
    })
  }

  def zipType:Rule0 = rule{
    "snappy" | "gzip" | "deflate" | "lz4"
  }

  def zipEncoder:Rule1[Option[FieldEncoder]] = rule {
    "zip" ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~ encoder ~ mayWhiteSpace ~ "," ~ mayWhiteSpace ~ capture(zipType) ~ mayWhiteSpace ~ ")" ~> ((d:Option[FieldEncoder], t:String)⇒{
      d match {
        case None⇒push(None)
        case Some(encoder) ⇒ push(Option.apply(new ZipFieldEncoder(encoder,t.toZipStrategy)))
      }
    })
  }

  def inputLine:Rule1[Option[FieldEncoder]] = rule {
    zipEncoder | encoder
  }
}

object EncoderParser{
  def apply(input: ParserInput, dynamicEncoders: Map[String, FieldEncoder]):Either[Exception,FieldEncoder] = {
    val parser = new EncoderParser(input,dynamicEncoders)
    val result = parser.inputLine.run()
    result match {
      case Success(value) ⇒ Right(value.get)
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}


