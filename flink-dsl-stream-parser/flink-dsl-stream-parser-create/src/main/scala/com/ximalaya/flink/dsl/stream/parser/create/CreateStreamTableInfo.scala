package com.ximalaya.flink.dsl.stream.parser.create

import java.lang.reflect.Constructor

import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.side.CastErrorStrategy
import com.ximalaya.flink.dsl.stream.parser._

/**
  *
  * @author martin.dong
  *
  **/
sealed class CreateStreamTableInfo(fields:Seq[SourceField],
                                   key:Option[String],
                                   waterMarkStrategy: Option[WaterMarkStrategy]) {
      require(checkKeyLegal)
      require(checkWatermarkStrategyLegal)

      def checkKeyLegal:Boolean = {
          key match {
            case None⇒true
            case Some(p)⇒ !fields.exists(field⇒field.getAliasName.getOrElse(field.getFieldName)==p)
          }
      }

      def checkWatermarkStrategyColumn(column:String):Boolean ={
        fields.exists(field⇒field.getAliasName.getOrElse(field.getFieldName)==column &&
          field.getFieldType == FieldType.LONG)
      }
      def checkWatermarkStrategyLegal:Boolean = {
          waterMarkStrategy match {
            case None  ⇒ true
            case Some(w) ⇒ w match {
              case Ingestion ⇒ true
              case a:Ascending  ⇒ checkWatermarkStrategyColumn(a.columnName)
              case m:MaxOuterOfOrder ⇒ checkWatermarkStrategyColumn(m.columnName)
            }
          }
      }
}

class KafkaCreateStreamTableInfo(val tableName:String,
                                 val fields:Seq[SourceField],
                                 val key:Option[String],
                                 val waterMarkStrategy: Option[WaterMarkStrategy],
                                 val dataType:MessageDecoder[_],
                                 val keyDecoder:FieldDecoder,
                                 val parallel:Int,
                                 val castError: CastErrorStrategy,
                                 optional:Map[String,String]) extends CreateStreamTableInfo(fields,
  key,
  waterMarkStrategy){

  //check parameters

  //optional must contain topic,zookeeper,broker,offset and groupId
  require(optional.contains("topic") &&
            optional.contains("zookeeper") &&
            optional.contains("broker") &&
            optional.contains("offset") &&
            optional.contains("groupId"))

  //kafka parameters
          val topic:String = optional("topic").trim
          val zookeeper:String = optional("zookeeper").trim
          val broker:String = optional("broker").trim
          val offset:Long = optional("offset").trim.toLong
          val groupId:String = optional("groupId").trim

  override def toString:String = s"KafkaCreateStreamTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"waterMarkStrategy=$waterMarkStrategy dataType=$dataType keyDecoder=$keyDecoder parallel=$parallel castError=$castError " +
    s"topic=$topic zookeeper=$zookeeper broker=$broker offset = $offset groupId=$groupId)"

  override def equals(obj: scala.Any): Boolean = {
     obj match {
       case other:KafkaCreateStreamTableInfo⇒
         this.tableName == other.tableName &&
         this.fields == other.fields &&
         this.key == other.key &&
         this.waterMarkStrategy == other.waterMarkStrategy  &&
         this.dataType == other.dataType &&
         this.parallel == other.parallel &&
         this.keyDecoder == other.keyDecoder &&
         this.castError == other.castError &&
         this.topic == other.topic &&
         this.zookeeper == other.zookeeper &&
         this.broker == other.broker &&
         this.offset == other.offset &&
         this.groupId == other.groupId
       case _ ⇒ false
     }
  }
}

class HDFSCreateStreamTableInfo(val tableName:String, val fields:Seq[SourceField],
                                val key:Option[String],
                                val waterMarkStrategy: Option[WaterMarkStrategy],
                                val dataType:MessageDecoder[_],
                                val keyDecoder:FieldDecoder,
                                val parallel:Int,
                                val castError: CastErrorStrategy,
                                optional:Map[String,String]) extends CreateStreamTableInfo(fields,
  key,
  waterMarkStrategy){

  //check parameters

  require(optional.contains("path") &&
    optional.contains("fsDefaultName") &&
    optional.contains("zip"))

  //hdfs parameters
  val path:String = optional("path").trim
  val fsDefaultName:String = optional("fsDefaultName").trim
  val zip:String = optional("zip").trim

  override def toString:String = s"HDFSCreateStreamTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"waterMarkStrategy=$waterMarkStrategy dataType=$dataType keyDecoder=$keyDecoder parallel=$parallel castError=$castError " +
    s"path=$path fsDefaultName=$fsDefaultName zip=$zip)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other:HDFSCreateStreamTableInfo⇒
        this.tableName == other.tableName &&
        this.fields == other.fields &&
          this.key == other.key &&
          this.waterMarkStrategy == other.waterMarkStrategy  &&
          this.dataType == other.dataType &&
          this.keyDecoder == other.keyDecoder &&
          this.parallel == other.parallel &&
          this.castError == other.castError &&
          this.path == other.path &&
          this.fsDefaultName == other.fsDefaultName &&
          this.zip == other.zip
      case _ ⇒ false
    }
  }
}

class SocketCreateStreamTableInfo(val tableName:String, val fields:Seq[SourceField],
                                  val key:Option[String],
                                  val waterMarkStrategy: Option[WaterMarkStrategy],
                                  val dataType:MessageDecoder[_],
                                  val keyDecoder:FieldDecoder,
                                  val parallel:Int,
                                  val castError: CastErrorStrategy,
                                  optional:Map[String,String]) extends CreateStreamTableInfo(fields,
  key,
  waterMarkStrategy){

  //check parameters

  require(optional.contains("ip") &&
    optional.contains("port") &&
    optional.contains("maxRetry"))


  val ip:String = optional("ip").trim
  val port:Int = optional("port").trim.toInt
  val maxRetry:Int = optional("maxRetry").trim.toInt


  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other:SocketCreateStreamTableInfo⇒
          this.tableName == other.tableName &&
          this.fields == other.fields &&
          this.key == other.key &&
          this.waterMarkStrategy == other.waterMarkStrategy  &&
          this.dataType == other.dataType &&
          this.keyDecoder == other.keyDecoder &&
          this.parallel == other.parallel &&
          this.castError == other.castError &&
          this.ip == other.ip &&
          this.port == other.port &&
          this.maxRetry == other.maxRetry
      case _ ⇒ false
    }
  }

  override def toString: String =  s"SocketCreateStreamTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"waterMarkStrategy=$waterMarkStrategy dataType=$dataType keyDecoder=$keyDecoder parallel=$parallel castError=$castError " +
    s"ip=$ip port=$port maxRetry=$maxRetry)"

}

class RabbitMQCreateStreamTableInfo(val tableName:String, val fields:Seq[SourceField],
                                    val key:Option[String],
                                    val waterMarkStrategy: Option[WaterMarkStrategy],
                                    val dataType:MessageDecoder[_],
                                    val keyDecoder:FieldDecoder,
                                    val parallel:Int,
                                    val castError: CastErrorStrategy,
                                    optional:Map[String,String]) extends CreateStreamTableInfo(fields,
  key,
  waterMarkStrategy){


  //check parameters

  require(optional.contains("host") &&
      optional.contains("port") &&
      optional.contains("queue") &&
      optional.contains("useCorrelationIds"))

  val host:String = optional("host").trim
  val port:Int = optional("port").trim.toInt
  val queue:String = optional("queue").trim
  val useCorrelationIds:Boolean = optional("useCorrelationIds").trim.toBoolean

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other:RabbitMQCreateStreamTableInfo⇒
          this.tableName == other.tableName &&
          this.fields == other.fields &&
          this.key == other.key &&
          this.waterMarkStrategy == other.waterMarkStrategy  &&
          this.dataType == other.dataType &&
          this.keyDecoder == other.keyDecoder &&
          this.parallel == other.parallel &&
          this.castError == other.castError &&
          this.host == other.host &&
          this.port == other.port &&
          this.useCorrelationIds == other.useCorrelationIds &&
          this.queue == other.queue
      case _ ⇒ false
    }
  }

  override def toString: String =  s"RabbitMQCreateStreamTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"waterMarkStrategy=$waterMarkStrategy dataType=$dataType keyDecoder=$keyDecoder parallel=$parallel castError=$castError " +
    s"host=$host port=$port queue=$queue useCorrelationIds=$useCorrelationIds)"
}

object CreateStreamTableInfo{

  val parameters:Array[Class[_]] = Array(classOf[String],
    classOf[Seq[SourceField]],
    classOf[Option[String]],
    classOf[Option[WaterMarkStrategy]],
    classOf[MessageDecoder[AnyRef]],
    classOf[FieldDecoder],
    classOf[Int],
    classOf[CastErrorStrategy],
    classOf[Map[String,String]])

  val sourceManager:Map[String,Constructor[_]]=Map(
    "kafka"→ classOf[KafkaCreateStreamTableInfo].getConstructor(parameters:_*),
    "hdfs" → classOf[HDFSCreateStreamTableInfo].getConstructor(parameters:_*),
    "socket" → classOf[SocketCreateStreamTableInfo].getConstructor(parameters:_*),
    "rabbitmq" → classOf[RabbitMQCreateStreamTableInfo].getConstructor(parameters:_*)
  )

  def apply(tableName:String,
            field: Seq[SourceField],
            pw:KeyAndWaterMark,
            required: StreamRequired,
            optional:Map[String,String],
            dynamicDecoders:Map[String,FieldDecoder],
            dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]): CreateStreamTableInfo = {
    var key:Option[String] = None
    var waterMarkStrategy:Option[WaterMarkStrategy] = None
    pw match {
      case Both(p,w)⇒
          key = Some(p)
          waterMarkStrategy = Some(w)
      case KeyOnly(p)⇒
          key = Some(p)
      case WaterMarkOnly(w)⇒
          waterMarkStrategy = Some(w)
      case _ ⇒
    }
    sourceManager(required.source).newInstance(tableName,field,key,waterMarkStrategy,
      required.dataType.toSourceDataType(dynamicSourceDataType),required.keyDecoder.toDecoder(dynamicDecoders),Int.box(required.parallel),
      required.castError,optional).asInstanceOf[CreateStreamTableInfo]
  }
}
