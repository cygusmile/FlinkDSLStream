package com.ximalaya.flink.dsl.stream.factory

import com.alibaba.fastjson.JSON
import com.ximalaya.flink.dsl.stream.api.field.decoder.{FieldDecoder, RawFieldDecoder, StringFieldDecoder, UnzipFieldDecoder}
/**
  *
  * @author martin.dong
  *
  **/

object FieldDecodeFactory {

  /**
    * create field formatter
    * @param dslExpr dsl expression
    * @return
    */
  def createDecoder(dslExpr:String):Option[FieldDecoder]={
//    Option.apply(FormatterType.noSqlfFMaps.get(dslExpr)).map(formatter⇒{
//        formatter match {
//          case FormatterType.RAW ⇒ new RawFieldDecoder
//          case FormatterType.STRING ⇒ new StringFieldDecoder
//          case _ ⇒ throw new RuntimeException(s"illegal dsl expr : $dslExpr")
//        }
//    })
     null
  }

  /**
    * create unzip field formatter
    * @param dslExpr dsl expression
    * @param unzipStrategy unzip strategy
    * @return
    */
  def createUnzipDecoder(dslExpr:String,
                           unzipStrategy:String):Option[FieldDecoder]={
      createDecoder(dslExpr).map(new UnzipFieldDecoder(_,null))
  }

}
