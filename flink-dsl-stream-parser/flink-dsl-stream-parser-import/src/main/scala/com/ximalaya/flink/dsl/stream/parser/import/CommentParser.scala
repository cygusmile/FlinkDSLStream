package com.ximalaya.flink.dsl.stream.parser.`import`

import com.ximalaya.flink.dsl.stream.parser.DslStreamParser
import org.parboiled2.{CharPredicate, ParserInput, Rule0, Rule1}

import scala.util.{Failure, Success}

/**
  *
  * @author martin.dong
  *
  **/

class CommentParser(val input:ParserInput) extends DslStreamParser {
  /**
    * 解析动作
    *
    * @return
    */
  override def action: Rule0 = rule{
    "--"
  }

  def inputLine:Rule0 = rule {
    mayWhiteSpace ~ action ~ (CharPredicate.Printable++'\n').*  ~ EOI
  }
}

object CommentParser{
  def apply(input: ParserInput):Option[Exception]={
      val parser = new CommentParser(input)
      val result=parser.inputLine.run()
      result match {
        case Success(_)⇒None
        case Failure(throwable)⇒ Some(parser.convertError(throwable).left.get)
      }
  }
}