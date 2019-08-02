package com.ximalaya.flink.dsl.stream.factory

import com.ximalaya.flink.dsl.stream.api.message.decoder._

/**
  *
  * @author martin.dong
  *
  **/

object SourceDataTypeFactory {

  def createSourceFormatter(dslExpr:String):Option[MessageDecoder[Array[Byte]]]={
//    Option.apply(FormatterType.messageQueueSourceFmap.get(dslExpr)).map(formatter⇒{
//      formatter match {
//        case FormatterType.CSV⇒ new CsvMessageDecoder
//        case FormatterType.JSON⇒new JsonMessageDecoder
//        case _ ⇒ throw new RuntimeException(s"illegal dsl expr : $dslExpr")
//      }
//    })
    null
  }

  def createUnzipSourceFormatter(dslExpr:String,
                                 unzipStrategy:String):Option[MessageDecoder[Array[Byte]]]={
      createSourceFormatter(dslExpr).map(formatter⇒new UnzipMessageDecoder(formatter,null))
  }


}
