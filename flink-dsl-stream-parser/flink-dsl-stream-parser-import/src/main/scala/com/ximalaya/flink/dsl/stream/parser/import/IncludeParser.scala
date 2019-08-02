package com.ximalaya.flink.dsl.stream.parser.`import`

import com.ximalaya.flink.dsl.stream.parser.DslStreamParser
import org.parboiled2._

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class IncludeParser(val input:ParserInput) extends DslStreamParser {
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    "include"
  }

  def inputLine:Rule1[String] = rule {
    mayWhiteSpace ~ action ~ mustWhiteSpace ~ capture(CharPredicate.Printable.+) ~ mayWhiteSpace ~ EOI
  }
}

object IncludeParser{
  def apply(input: ParserInput):Either[Exception,String]={
      val parser = new IncludeParser(input)
      val result = parser.inputLine.run()
      result match {
        case Success(value)⇒Right(value)
        case Failure(throwable)⇒ parser.convertError(throwable)
      }
  }
}