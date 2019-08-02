package com.ximalaya.flink.dsl.stream.parser

import com.ximalaya.flink.dsl.stream.`type`.{SinkDataType, SourceDataType}
import com.ximalaya.flink.dsl.stream.api.message.encoder._
import org.parboiled2._

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class SinkDataTypeParser(val input:ParserInput,val dynamicDataTypes:Map[String,MessageEncoder]) extends DslStreamParser {

  /**
    * 匹配单个标识符 并将路径名推入值栈
    * @return
    */
  def sourceDataType:Rule1[Option[MessageEncoder]] = rule {
    capture(oneOrMore(CharPredicate.AlphaNum.+("_$"))) ~> ((identity:String)⇒{
      val buildInDecoder = Option.apply(SinkDataType.get(identity))
      buildInDecoder match {
        case Some(_)⇒push(buildInDecoder)
        case None⇒ push(dynamicDataTypes.get(identity))
      }
    })
  }

  def zipType:Rule0 = rule{
    "snappy" | "gzip" | "deflate" | "lz4"
  }

  def unzipDecoder:Rule1[Option[MessageEncoder]] = rule {
    "unzip" ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~ sourceDataType ~ mayWhiteSpace ~ "," ~ mayWhiteSpace ~ capture(zipType) ~
      mayWhiteSpace ~ ")" ~> ((msd:Option[MessageEncoder], t:String)⇒{
      msd match {
        case None⇒push(None)
        case Some(m) ⇒
          push(Option.apply(new ZipMessageEncoder(m,t.toZipStrategy)))
      }
    })
  }

  def inputLine:Rule1[Option[MessageEncoder]] = rule {
    unzipDecoder | sourceDataType
  }
}

object SinkDataTypeParser{
  def apply(input: ParserInput,dynamicDataTypes:Map[String,MessageEncoder]):Either[Exception,MessageEncoder] = {
    val parser = new SinkDataTypeParser(input,dynamicDataTypes)
    val result = parser.inputLine.run()
    result match {
      case Success(value) ⇒ Right(value.get)
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}

