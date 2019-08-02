package com.ximalaya.flink.dsl.stream.parser.query

import com.ximalaya.flink.dsl.stream.parser.DslStreamParser
import org.parboiled2.{CharPredicate, ParserInput, Rule0, Rule1}
import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class QueryParser(val input:ParserInput)  extends DslStreamParser{
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    "select"
  }

  def queryBody:Rule0=rule{
    !(mustWhiteSpace ~ as ~ mustWhiteSpace ~ oneOrMore(CharPredicate.AlphaNum.+("_$")) ~ mayWhiteSpace ~  EOI)
  }

  def queryAs(cursor:Int):String={
      input.sliceString(cursor,input.length).trim.drop(2).trim
  }

  def inputLine:Rule1[(String,String)] = rule{
    capture( oneOrMore(queryBody ~ ANY) ) ~> ((body:String)⇒{
      push((body.trim,queryAs(cursor)))
    })
  }
}

object QueryParser{
  def apply(input: ParserInput):Either[Exception,(String,String)]={
      val parser = new QueryParser(input)
      val result = parser.inputLine.run()
    result match {
      case Success(value)⇒Right(value)
      case Failure(throwable)⇒ parser.convertError(throwable)
    }
  }
}
