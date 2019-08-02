package com.ximalaya.flink.dsl.stream

/**
  *
  * @author martin.dong
  **/
import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Function

import com.google.common.base.Optional
import com.google.common.collect.{Lists, Maps}
import com.ximalaya.flink.dsl.stream.`type`.{Decoder, FieldType}
import com.ximalaya.flink.dsl.stream.api.message.encoder.MessageEncoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.side.CastErrorStrategy

import scala.concurrent.duration.TimeUnit

/**
  *
  * @author martin.dong
  *
  **/

package object parser {

  implicit class WrapOptional[T<:AnyRef](val value:Optional[T]){
    def toScalaOption:Option[T]={
        Option.apply(value.orNull())
    }
  }

  implicit class WrapMap[T<:AnyRef](val map:Map[String,T]){

    def toJavaMap:util.Map[String,T]={
      val javaMap=Maps.newHashMap[String,T]()
      map.foreach{
        case (k,v)⇒javaMap.put(k,v)
      }
      javaMap
    }
  }

  implicit class WrapSeq[T<:AnyRef](val value:Seq[T]){

    def splitPaths:(Option[List[String]],String)={
        if(value.length==1){
          (None,value.head.toString)
        }else{
          (Some(value.dropRight(1).map(_.toString).toList),
            value.last.toString)
        }
    }

    def toJavaList:util.List[T]={
      val javaList = Lists.newArrayList[T]()
      value.foreach(e⇒javaList.add(e))
      javaList
    }
  }

  implicit class WrapString(val value:String){

    def toTimeUnit:TimeUnit={
      value match {
        case "ms" ⇒ TimeUnit.MILLISECONDS
        case "s" ⇒ TimeUnit.SECONDS
        case "m" ⇒ TimeUnit.MINUTES
        case "h" ⇒ TimeUnit.HOURS
        case "d" ⇒ TimeUnit.DAYS
      }
    }

    def toDecoder(dynamicDecoders:Map[String,FieldDecoder]):FieldDecoder={
        DecoderParser(value,dynamicDecoders) match {
          case Left(e) ⇒ throw e
          case Right(decoder) ⇒ decoder
        }
    }

    def toEncoder(dynamicEncoders:Map[String,FieldEncoder]):FieldEncoder={
      EncoderParser(value,dynamicEncoders) match {
        case Left(e) ⇒ throw e
        case Right(encoder) ⇒ encoder
      }
    }

    def toSinKDataType(dynamicDataTypes:Map[String,MessageEncoder]):MessageEncoder={
      SinkDataTypeParser(value,dynamicDataTypes) match {
        case Left(e) ⇒ throw e
        case Right(dataType) ⇒ dataType
      }
    }

    def toCastErrorStrategy:CastErrorStrategy = {
        CastErrorStrategy(value)
    }

    def toSourceDataType(dynamicDataTypes:Map[String,MessageDecoder[AnyRef]]):MessageDecoder[AnyRef]={
      SourceDataTypeParser(value,dynamicDataTypes) match {
        case Left(e) ⇒ throw e
        case Right(dataType) ⇒ dataType
      }
    }

    def toUnzipStrategy: Function[Array[Byte], Array[Byte]]= {
      value match{
        case "gzip"⇒new Function[Array[Byte],Array[Byte]]{
          override def apply(t: Array[Byte]): Array[Byte] = t
          override def hashCode(): Int = value.hashCode
          override def equals(obj: scala.Any): Boolean = obj.toString==value
          override def toString: String = value
        }
      }
    }

    def toZipStrategy: Function[Array[Byte], Array[Byte]]= ???

    def toFieldType:FieldType={
        FieldType.get(value)
    }

    def cleanString(shouldDrop:Boolean = false):String = if(shouldDrop){
      value.trim.drop(1).dropRight(1).trim
    }else{
      value.trim
    }

    def parseDefault(fieldType: FieldType):AnyRef = {
      def parseBool(default:String,box:Boolean):Any={
        default match {
          case "true" | "false" ⇒ if(box) Boolean.box(default.toBoolean) else default.toBoolean
          case _ ⇒ throw new RuntimeException(s"boolean type only supports default values for true or false: $default")
        }
      }
      def parseNull(fieldType:  FieldType, default:String):AnyRef={
        default match {
          case "null" ⇒ null
          case _ ⇒ throw new RuntimeException(s"${fieldType.getTypeName} type only supports default values for null: $default")
        }
      }
      def parseDecimal(fieldType:  FieldType, default:String,box:Boolean): Any={
        "^[-+]?[0-9]{1,}[.][0-9]*$".r.findFirstMatchIn(default) match {
          case Some(_) ⇒ if(fieldType == FieldType.DOUBLE || fieldType == FieldType.DOUBLE_ARRAY) {
            if(box) Double.box(default.toDouble) else default.toDouble
          } else {
            if(box) Float.box(default.toFloat) else default.toFloat
          }
          case None ⇒  throw new RuntimeException(s"${fieldType.getTypeName} type must be set decimal default value: $default")
        }
      }
      def parseInteger(fieldType:  FieldType, default:String,box:Boolean):Any={
        "^[-+]?\\d+$".r.findFirstMatchIn(default) match{
          case Some(_)⇒ if(fieldType == FieldType.LONG || fieldType == FieldType.LONG_ARRAY) {
            if(box) Long.box(default.toLong) else default.toLong
          }else {
            if(box) Int.box(default.toInt) else default.toInt
          }
          case None ⇒ throw new RuntimeException(s"${fieldType.getTypeName} type must be set integer default value: $default")
        }
      }
      def parseString(fieldType: FieldType, default:String):AnyRef={
        "^'.*'$".r.findFirstMatchIn(default) match {
          case Some(_)⇒  default.toString.cleanString(true)
          case None ⇒ throw new RuntimeException(s"${fieldType.getTypeName} type must be set string default value: $default")
        }
      }
      def parseArray(fieldType: FieldType, default:String):AnyRef={
        "^\\[.*\\]$".r.findFirstMatchIn(default) match {
          case Some(v) ⇒
            fieldType match {
              case FieldType.BYTE_ARRAY | FieldType.STRING_ARRAY | FieldType.OBJECT_ARRAY ⇒
                require(default.trim == "")
                if (fieldType == FieldType.BYTE_ARRAY) {
                  Array[Byte]()
                } else if (fieldType == FieldType.STRING_ARRAY) {
                  Array[String]()
                } else {
                  Array[AnyRef]()
                }
              case FieldType.DOUBLE_ARRAY | FieldType.FLOAT_ARRAY ⇒
                val array = default.dropRight(1).drop(1).trim.split(",").map(f ⇒ parseDecimal(fieldType, f.trim, box = false))
                if (fieldType == FieldType.DOUBLE_ARRAY) {
                  array.map(_.asInstanceOf[Double])
                } else {
                  array.map(_.asInstanceOf[Float])
                }
              case FieldType.INT_ARRAY | FieldType.LONG_ARRAY ⇒
                val array = default.dropRight(1).drop(1).trim.split(",").map(f ⇒ parseInteger(fieldType, f.trim, box = false))
                if (fieldType == FieldType.INT_ARRAY) {
                  array.map(_.asInstanceOf[Int])
                } else {
                  array.map(_.asInstanceOf[Long])
                }
            }
          case None ⇒ throw new RuntimeException(s"${fieldType.getTypeName} type must be set ${fieldType.getTypeName} array default value: $default")
        }
      }
      def parseMap(fieldType: FieldType, default:String):AnyRef={
        "^{\\s*}$".r.findFirstMatchIn(default) match {
          case Some(_) ⇒ Map[AnyRef,AnyRef]()
          case None ⇒ throw new RuntimeException(s"${fieldType.getTypeName} type only supports default value for {}: $default")
        }
      }

      fieldType match {
        case FieldType.BOOL ⇒ parseBool(value,box = true).asInstanceOf[AnyRef]
        case FieldType.BYTE | FieldType.OBJECT ⇒ parseNull(fieldType,value)
        case FieldType.DOUBLE | FieldType.FLOAT ⇒ parseDecimal(fieldType,value,box = true).asInstanceOf[AnyRef]
        case FieldType.INT | FieldType.LONG ⇒ parseInteger(fieldType,value,box = true).asInstanceOf[AnyRef]
        case FieldType.STRING ⇒ parseString(fieldType,value)
        case FieldType.MAP ⇒ parseMap(fieldType,value)
        case FieldType.BYTE_ARRAY | FieldType.DOUBLE_ARRAY | FieldType.FLOAT_ARRAY |
             FieldType.INT_ARRAY | FieldType.LONG_ARRAY | FieldType.OBJECT_ARRAY | FieldType.STRING_ARRAY  ⇒ parseArray(fieldType,value)
      }
    }
  }
}
