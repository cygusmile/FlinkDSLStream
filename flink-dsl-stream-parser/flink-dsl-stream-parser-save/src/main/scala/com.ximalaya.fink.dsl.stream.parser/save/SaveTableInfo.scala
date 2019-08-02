package com.ximalaya.fink.dsl.stream.parser.save

import java.lang.reflect.Constructor

import com.ximalaya.flink.dsl.stream.api.message.encoder.MessageEncoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.parser.{HBaseSinkField, RedisHashSinkField, SinkField}
import com.ximalaya.flink.dsl.stream.parser._
/**
  *
  * @author martin.dong
  *
  **/
sealed class SaveTableInfo

object SaveTableInfo{

  val parameters:Array[Class[_]] = Array(classOf[String],
    classOf[Seq[SinkField]],
    classOf[CacheStrategy],
    classOf[Option[String]],
    classOf[FieldEncoder],
    classOf[Int],
    classOf[Map[String,String]],classOf[Map[String,FieldEncoder]],classOf[Map[String,MessageEncoder]])

  val saveManage:Map[String,Constructor[_]] = Map(
    "kafka" → classOf[KafkaSaveTableInfo].getConstructor(parameters:_*),
    "hbase" → classOf[HBaseSaveTableInfo].getConstructor(parameters:_*),
    "debug" → classOf[DebugSaveTableInfo].getConstructor(parameters:_*),
    "dummy" → classOf[DummySaveTableInfo].getConstructor(parameters:_*),
    "mysql" → classOf[MysqlSaveTableInfo].getConstructor(parameters:_*),
    "redis-string" → classOf[RedisStringSaveTableInfo].getConstructor(parameters:_*),
    "redis-hash" → classOf[RedisHashSaveTableInfo].getConstructor(parameters:_*)
  )

  def apply(tableName:String, field: Seq[SinkField], kc:KeyAndCache,
            required:SaveRequired, optional:Map[String,String],
            dynamicEncoders:Map[String,FieldEncoder],
            dynamicSinkDataType:Map[String,MessageEncoder]): SaveTableInfo = {
    var key:Option[String] = None
    var cacheStrategy:CacheStrategy = null
    kc match {
      case Both(k,c)⇒
        key = Some(k)
        cacheStrategy = c
      case CacheOnly(c)⇒
        cacheStrategy = c
    }

    required.sink match {
      case "kafka" | "hbase" | "debug" | "dummy" ⇒ saveManage(required.sink).newInstance(tableName,
        field,cacheStrategy,key,
        required.keyEncoder.toEncoder(dynamicEncoders),Int.box(required.parallel),optional,
        dynamicEncoders,dynamicSinkDataType).asInstanceOf[SaveTableInfo]
      case "redis"⇒
        saveManage(s"${required.sink}-${optional("dataType")}").newInstance(tableName,
          field,cacheStrategy,key,
          required.keyEncoder.toEncoder(dynamicEncoders),Int.box(required.parallel),optional,
          dynamicEncoders,dynamicSinkDataType).asInstanceOf[SaveTableInfo]
    }
  }
}

class KafkaSaveTableInfo(val tableName:String,
                         val fields:Seq[SinkField],
                         val cacheStrategy: CacheStrategy,
                         val key:Option[String],
                         val keyEncoder:FieldEncoder,
                         val parallel:Int,
                         optional:Map[String,String],
                         dynamicEncoders:Map[String,FieldEncoder],
                         dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo{

  //check parameters

  //optional must contain topic,zookeeper,broker,offset and groupId
  require(optional.contains("topic") &&
    optional.contains("broker") &&
    optional.contains("dataType"))

  //kafka parameters
  val topic:String = optional("topic").trim
  val broker:String = optional("broker").trim
  val dataType:MessageEncoder = optional("dataType").trim.toSinKDataType(dynamicSinkDataType)

  override def equals(obj: scala.Any): Boolean = obj match {
    case other:KafkaSaveTableInfo ⇒
      this.tableName == other.tableName &&
        this.fields == other.fields &&
      this.cacheStrategy == other.cacheStrategy &&
        this.key == other.key &&
        this.keyEncoder == other.keyEncoder &&
      this.parallel == other.parallel &&
        this.topic == other.topic &&
        this.broker == other.broker &&
        this.dataType == other.dataType
  }

  override def toString: String = s"KafkaSaveTableInfo(tableName=$tableName fields=${fields.mkString(",")} " +
    s"cacheStrategy=$cacheStrategy key=$key " +
    s"keyEncoder=$keyEncoder parallel=$parallel cacheStrategy=$cacheStrategy topic=$topic broker=$broker dataType=$dataType)"
}

class HBaseSaveTableInfo(val tableName:String,
                         fields:Seq[SinkField],
                         val cacheStrategy: CacheStrategy,
                         val key:Option[String],
                         val keyEncoder:FieldEncoder,
                         val parallel:Int,
                         optional:Map[String,String],
                         dynamicEncoders:Map[String,FieldEncoder],
                         dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo{
  //check fields
  fields.foreach(field⇒
    require(field.path.isDefined && field.path.get.size==1)
  )

  //hbase fields
  val hbaseFields:Seq[HBaseSinkField] = fields.map(field⇒
    HBaseSinkField(field.name,field.path.get.head,field.columnName)
  )

  //check parameters
  require(optional.contains("zookeeper") &&
    optional.contains("tableName") &&
    optional.contains("encoder"))

  //hbase parameters
  val zookeeper: String = optional("zookeeper").trim
  val hbaseTableName: String = optional("tableName").trim
  val encoder: FieldEncoder = optional("encoder").trim.toEncoder(dynamicEncoders)

  override def equals(obj: scala.Any): Boolean = obj match {
    case other:HBaseSaveTableInfo ⇒
      this.tableName == other.tableName &&
        this.hbaseFields == other.hbaseFields &&
      this.cacheStrategy == other.cacheStrategy &&
        this.key == other.key &&
        this.keyEncoder == other.keyEncoder &&
      this.parallel == other.parallel &&
        this.zookeeper == other.zookeeper &&
        this.hbaseTableName == other.hbaseTableName &&
        this.encoder == other.encoder
  }

  override def toString: String = s"HBaseSaveTableInfo(tableName=$tableName fields=${hbaseFields.mkString(",")} " +
    s"cacheStrategy=$cacheStrategy key=$key " +
    s"keyEncoder=$keyEncoder parallel=$parallel zookeeper=$zookeeper hbaseTableName=$hbaseTableName encoder=$encoder)"
}

class RedisStringSaveTableInfo(val tableName:String,
                               fields:Seq[SinkField],
                               val cacheStrategy: CacheStrategy,
                               val key:Option[String],
                               val keyEncoder:FieldEncoder,
                               val parallel:Int,
                               optional:Map[String,String],
                               dynamicEncoders:Map[String,FieldEncoder],
                               dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo {
  //check parameters
  require(optional.contains("url") &&
    optional.contains("password") &&
    optional.contains("username") &&
    optional.contains("database") &&
    optional.contains("encoder"))


  val redisStringField:String = fields.head.name

  //redis parameters
  val url :String = optional("url").trim
  val password :String = optional("password").trim
  val username :String = optional("username").trim
  val database :String = optional("database").trim
  val encoder :FieldEncoder = optional("encoder").trim.toEncoder(dynamicEncoders)

  override def toString: String = s"RedisStringSaveTableInfo(tableName=$tableName redisStringField=$redisStringField" +
    s"cacheStrategy=$cacheStrategy key=$key keyEncoder=$keyEncoder " +
    s"url=$url password=$password username=$username database=$database encoder=$encoder)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: RedisStringSaveTableInfo ⇒
        this.tableName == other.tableName &&
        this.cacheStrategy == other.cacheStrategy
          this.redisStringField == other.redisStringField &&
          this.key == other.key &&
          this.url == other.url &&
          this.password == other.password &&
          this.username == other.username &&
          this.database == other.database &&
          this.encoder == other.encoder
      case _ ⇒ false
    }
  }
}

class RedisHashSaveTableInfo(val tableName:String,
                             val fields:Seq[SinkField],
                             val cacheStrategy: CacheStrategy,
                             val key:Option[String],
                             val keyEncoder:FieldEncoder,
                             val parallel:Int,
                             optional:Map[String,String],
                             dynamicEncoders:Map[String,FieldEncoder],
                             dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo {
  //check parameters
  require(optional.contains("url") &&
    optional.contains("password") &&
    optional.contains("username") &&
    optional.contains("database") &&
    optional.contains("encoder"))

  val redisHashFields:Seq[RedisHashSinkField] = fields.map(field⇒RedisHashSinkField(field.name,field.columnName))

  //redis parameters
  val url :String = optional("url").trim
  val password :String = optional("password").trim
  val username :String = optional("username").trim
  val database :String = optional("database").trim
  val encoder :FieldEncoder = optional("encoder").trim.toEncoder(dynamicEncoders)

  override def toString: String = s"RedisHashSaveTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key keyEncoder=$keyEncoder " +
    s"url=$url password=$password username=$username database=$database encoder=$encoder)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: RedisHashSaveTableInfo ⇒
        this.tableName == other.tableName &&
          this.redisHashFields == other.redisHashFields &&
          this.key == other.key &&
          this.url == other.url &&
          this.password == other.password &&
          this.username == other.username &&
          this.database == other.database &&
          this.encoder == other.encoder
      case _ ⇒ false
    }
  }

}

class MysqlSaveTableInfo(val tableName:String,
                         val fields:Seq[SinkField],
                         val cacheStrategy: CacheStrategy,
                         val key:Option[String],
                         val keyEncoder:FieldEncoder,
                         val parallel:Int,
                         optional:Map[String,String],
                         dynamicEncoders:Map[String,FieldEncoder],
                         dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo {
  //check parameters
  override def equals(obj: scala.Any): Boolean = obj match {
    case other:MysqlSaveTableInfo ⇒ this.tableName == other.tableName && this.fields == other.fields &&
        this.cacheStrategy == other.cacheStrategy && this.key == other.key && this.keyEncoder == this.keyEncoder &&
      this.parallel == other.parallel
  }

  override def toString: String = s"MysqlSaveTableInfo(tableName=$tableName fields=${fields.mkString(",")} " +
    s"cacheStrategy=$cacheStrategy key=$key keyEncoder=$keyEncoder parallel=$parallel)"
}

class DebugSaveTableInfo(val tableName:String,
                         val fields:Seq[SinkField],
                         val cacheStrategy: CacheStrategy,
                         val key:Option[String],
                         val keyEncoder:FieldEncoder,
                         val parallel:Int,
                         optional:Map[String,String],
                         dynamicEncoders:Map[String,FieldEncoder],
                         dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo {
  //check parameters
  //optional must contain topic,zookeeper,broker,offset and groupId
  require(optional.contains("formatter"))

  //数据编码方式
  val formatter:String =  optional("formatter")

  override def equals(obj: scala.Any): Boolean = obj match {
    case other:DebugSaveTableInfo ⇒ this.tableName == other.tableName && this.fields == other.fields &&
      this.cacheStrategy == other.cacheStrategy && this.key == other.key && this.keyEncoder == other.keyEncoder &&
      this.parallel == other.parallel && this.formatter == other.formatter
  }

  override def toString: String = s"HBaseSaveTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"keyEncoder=$keyEncoder parallel=$parallel cacheStrategy=$cacheStrategy formatter=$formatter)"
}

class DummySaveTableInfo(val tableName:String,
                         val fields:Seq[SinkField],
                         val cacheStrategy: CacheStrategy,
                         val key:Option[String],
                         val keyEncoder:FieldEncoder,
                         val parallel:Int,
                         optional:Map[String,String],
                         dynamicEncoders:Map[String,FieldEncoder],
                         dynamicSinkDataType:Map[String,MessageEncoder]) extends SaveTableInfo {
  //check parameters
  override def equals(obj: scala.Any): Boolean = obj match {
    case other:DummySaveTableInfo ⇒ this.tableName == other.tableName && this.fields == other.fields &&
      this.cacheStrategy == other.cacheStrategy && this.key == other.key && this.keyEncoder == other.keyEncoder &&
      this.parallel == other.parallel
  }

  override def toString: String = s"DummySaveTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"keyEncoder=$keyEncoder parallel=$parallel cacheStrategy=$cacheStrategy)"
}