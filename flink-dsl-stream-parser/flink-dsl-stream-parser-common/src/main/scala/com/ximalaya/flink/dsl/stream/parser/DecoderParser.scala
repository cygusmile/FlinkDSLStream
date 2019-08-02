package com.ximalaya.flink.dsl.stream.parser

import com.ximalaya.flink.dsl.stream.`type`.Decoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.{FieldDecoder, UnzipFieldDecoder}
import org.parboiled2.{CharPredicate, ParserInput, Rule0, Rule1}

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class DecoderParser(val input:ParserInput,val dynamicDecoders:Map[String,FieldDecoder]) extends DslStreamParser {


  /**
    * 匹配单个标识符 并将路径名推入值栈
    * @return
    */
  def decoder:Rule1[Option[FieldDecoder]] = rule {
    capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~> ((identity:String)⇒{
       val buildInDecoder = Option.apply(Decoder.get(identity))
       buildInDecoder match {
         case Some(_)⇒push(buildInDecoder)
         case None⇒ push(dynamicDecoders.get(identity))
       }
    })
  }

  def unzipType:Rule0 = rule{
    "snappy" | "gzip" | "deflate" | "lz4"
  }

  def unzipDecoder:Rule1[Option[FieldDecoder]] = rule {
     "unzip" ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~ decoder ~ mayWhiteSpace ~ "," ~ mayWhiteSpace ~ capture(unzipType) ~ mayWhiteSpace ~ ")" ~> ((d:Option[FieldDecoder],t:String)⇒{
        d match {
          case None⇒push(None)
          case Some(decoder) ⇒ push(Option.apply(new UnzipFieldDecoder(decoder,t.toUnzipStrategy)))
        }
      })
  }

  def inputLine:Rule1[Option[FieldDecoder]] = rule {
      unzipDecoder | decoder
  }
}

object DecoderParser{
  def apply(input: ParserInput, dynamicDecoders: Map[String, FieldDecoder]):Either[Exception,FieldDecoder] = {
      val parser = new DecoderParser(input,dynamicDecoders)
      val result = parser.inputLine.run()
      result match {
        case Success(value) ⇒ Right(value.get)
        case Failure(throwable)⇒ parser.convertError(throwable)
      }
  }
}
