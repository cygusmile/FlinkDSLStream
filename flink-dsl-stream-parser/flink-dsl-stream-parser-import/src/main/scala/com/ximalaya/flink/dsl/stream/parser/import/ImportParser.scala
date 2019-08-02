package com.ximalaya.flink.dsl.stream.parser.`import`

import com.ximalaya.flink.dsl.stream.`type`.FieldType
import com.ximalaya.flink.dsl.stream.parser.DslStreamParser
import org.parboiled2._
import com.ximalaya.flink.dsl.stream.parser._

import scala.util.{Failure, Success}
/**
  *
  * @author martin.dong
  *
  **/
class ImportParser(val input:ParserInput) extends DslStreamParser {
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    "import"
  }

  def withTypeValues:Rule1[List[(FieldType,String)]]=rule {
    "with" ~ mayWhiteSpace ~ "(" ~ mayWhiteSpace ~ keyValue.+.separatedBy(mayWhiteSpace~','~mayWhiteSpace) ~ mayWhiteSpace ~ ")" ~ mayWhiteSpace ~> ((kvs:Seq[(String,String)]) ⇒{
      push(kvs.map(kv⇒(kv._1.toFieldType,kv._2)).toList)
    })
  }

  def withNone:Rule1[List[(FieldType,String)]] = rule {
    capture(MATCH) ~> ((_:String)⇒push(List()))
  }

  def `with`:Rule1[List[(FieldType,String)]] = rule {
    withTypeValues | withNone
  }

  def inputLine= rule {
    mayWhiteSpace ~ action ~ mustWhiteSpace ~ capture(`type`) ~ mustWhiteSpace ~ "from" ~
    mustWhiteSpace ~ captureString ~ mustWhiteSpace ~ `with` ~ "as" ~ mustWhiteSpace ~
      captureString ~ mayWhiteSpace ~ EOI
  }

  def `type`:Rule0 = rule {
      ignoreCase("clazz") | ignoreCase("object") | ignoreCase("encoder") |
        ignoreCase("decoder") | ignoreCase("sinkDataType") | ignoreCase("sourceDataType") |
      ignoreCase("udf")
  }

}

object ImportParser{
    def apply(input: ParserInput):Either[Exception,ImportInfo]={
      val parser = new ImportParser(input)
      val result = parser.inputLine.run()
      result match {
        case Success(value)⇒
              val `type` = value.head
              val clazzName = value.tail.head.cleanString(true)
              val typeValues = value.tail.tail.head
              val udfName = value.tail.tail.tail.head.cleanString(true)
              Right(ImportInfo(`type`,clazzName,typeValues,udfName))
        case Failure(throwable)⇒ parser.convertError(throwable)
      }
  }
}

