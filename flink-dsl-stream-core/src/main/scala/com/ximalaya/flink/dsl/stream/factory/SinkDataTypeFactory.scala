package com.ximalaya.flink.dsl.stream.factory

import com.ximalaya.flink.dsl.stream.api.message.encoder._

/**
  *
  * @author martin.dong
  *
  **/

object SinkDataTypeFactory {

  /**
    * create sink formatter
    * @param dslExpr dsl expression
    * @return
    */
  def createSinkDataType(dslExpr:String):Option[MessageEncoder]={
//    Option.apply(FormatterType.messageQueueSinkFMap.get(dslExpr)).map(formatter⇒{
//      formatter match {
//        case FormatterType.JSON ⇒ new JsonMessageEncoder
//        case FormatterType.CSV⇒ new CsvMessageEncoder
//        case _ ⇒ throw new RuntimeException(s"illegal dsl expr : $dslExpr")
//      }
//    })
    null
  }

  /**
    * create zip sink formatter
    * @param dslExpr dsl expression
    * @param zipStrategy zip strategy
    * @return
    */
  def createZipSinkFormatter(dslExpr:String,
                             zipStrategy:String):Option[MessageEncoder]={
    createSinkDataType(dslExpr).map(new ZipMessageEncoder(_,
      null))
  }

}
