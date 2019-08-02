package com.ximalaya.flink.dsl.stream.parser.create

import java.lang.reflect.Constructor
import java.util.function.Function

import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.parser._

/**
  *
  * @author martin.dong
  *
  **/

sealed trait CreateBatchTableInfo
class HDFSCreateBatchTableInfo(val tableName:String, val fields:Seq[SourceField],
                               val key:Option[String],
                               source:String,
                               val keyDecoder:FieldDecoder,
                               val parallel:Int,
                               optional:Map[String,String],
                               dynamicDecoders:Map[String,FieldDecoder],
                               dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CreateBatchTableInfo {

  //check parameters
  require(source=="hdfs")
  require(optional.contains("path") &&
    optional.contains("fsDefaultName") &&
    optional.contains("zip") &&
    optional.contains("dataType"))

  //hdfs parameters
  val path:String = optional("path").trim
  val fsDefaultName:String = optional("fsDefaultName").trim
  val zip:Function[Array[Byte], Array[Byte]] = null
  val dataType:MessageDecoder[AnyRef]= optional("dataType").trim.toSourceDataType(dynamicSourceDataType)


  override def toString:String = s"HDFSCreateBatchTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"keyDecoder=$keyDecoder parallel=$parallel path=$path fsDefaultName=$fsDefaultName zip=$zip dataType=$dataType)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other:HDFSCreateBatchTableInfo⇒
        this.tableName == other.tableName &&
        this.fields == other.fields &&
          this.key == other.key &&
          this.keyDecoder == other.keyDecoder &&
          this.parallel == other.parallel &&
          this.path == other.path &&
          this.fsDefaultName == other.fsDefaultName &&
          this.zip == other.zip &&
          this.dataType == other.dataType
      case _ ⇒ false
    }
  }
}

class MysqlCreateBatchTableInfo(val tableName:String, fields:Seq[SourceField],
                                val key:Option[String],
                                source:String,
                                val keyDecoder:FieldDecoder,
                                val parallel:Int,
                                optional:Map[String,String],
                                dynamicDecoders:Map[String,FieldDecoder],
                                dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CreateBatchTableInfo {

  require(checkPrimaryKeyLegal)
  def checkPrimaryKeyLegal:Boolean = {
    key match {
      case None⇒true
      case Some(p)⇒fields.exists(field⇒field.getAliasName.getOrElse(field.getFieldName)==p)
    }
  }

  require(source=="mysql")
  //check fields
  fields.foreach(field⇒
    field.getPaths.isDefined && field.getDefaultValue.isEmpty
  )

  val mysqlFields:Seq[MysqlSourceField] = fields.map(field⇒MysqlSourceField(field.getFieldName,field.getAliasName))

  //check parameters
  require(source == "mysql")
  require(optional.contains("jdbcUrl") &&
    optional.contains("dbName") &&
    optional.contains("tableName") &&
    optional.contains("username") &&
    optional.contains("keyPattern") &&
    optional.contains("password"))

  //hbase parameters
  val jdbcUrl: String = optional("jdbcUrl").trim
  val dbName: String = optional("dbName").trim
  val mysqlTableName: String = optional("tableName").trim
  val userName: String = optional("username").trim
  val keyPattern:String = optional("keyPattern").trim
  val password:String = optional("password")

  override def toString: String = s"MysqlCreateBatchTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"keyDecoder=$keyDecoder parallel=$parallel jdbcUrl=$jdbcUrl dbName=$dbName " +
    s"tableName=$tableName userName=$userName keyPattern=$keyPattern password=$password)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: MysqlCreateBatchTableInfo ⇒
        this.mysqlTableName == other.mysqlTableName &&
        this.mysqlFields == other.mysqlFields &&
          this.key == other.key &&
          this.keyDecoder == other.keyDecoder &&
          this.parallel == other.parallel &&
          this.jdbcUrl == other.jdbcUrl &&
          this.dbName == other.dbName &&
          this.tableName == other.tableName &&
          this.userName == other.userName &&
          this.keyPattern == other.keyPattern &&
          this.password == other.password
      case _ ⇒ false
    }
  }
}
class HBaseCreateBatchTableInfo(val tableName:String, fields:Seq[SourceField],
                                val key:Option[String],
                                source:String,
                                val keyDecoder:FieldDecoder,
                                val parallel:Int,
                                optional:Map[String,String],
                                dynamicDecoders:Map[String,FieldDecoder],
                                dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]) extends CreateBatchTableInfo{

  require(source=="hbase")
  //check fields
  fields.foreach(field⇒
    require(field.getPaths.isDefined && field.getPaths.get.length==1)
  )

  //hbase fields
  val hbaseFields:Seq[HBaseSourceField] = fields.map(field⇒
    HBaseSourceField(field.getPaths.get.head,field.getFieldName,field.getAliasName,field.getFieldType,field.getDefaultValue)
  )

  //check parameters
  require(source == "hbase")
  require(optional.contains("zookeeper") &&
    optional.contains("tableName") &&
    optional.contains("keyPattern") &&
    optional.contains("decoder"))

  //hbase parameters
  val zookeeper: String = optional("zookeeper").trim
  val hbaseTableName: String = optional("tableName").trim
  val keyPattern: String = optional("keyPattern").trim
  val decoder: FieldDecoder = optional("decoder").trim.toDecoder(dynamicDecoders)

  override def toString: String = s"HBaseCreateBatchTableInfo(tableName=$tableName fields=${fields.mkString(",")} key=$key " +
    s"keyDecoder=$keyDecoder parallel=$parallel zookeepr=$zookeeper tableName=$tableName keyPattern=$keyPattern decoder=$decoder)"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case other: HBaseCreateBatchTableInfo ⇒
        this.tableName == other.tableName &&
        this.hbaseFields == other.hbaseFields &&
          this.key == other.key &&
          this.keyDecoder == other.keyDecoder &&
          this.parallel == other.parallel &&
          this.zookeeper == other.zookeeper &&
          this.tableName == other.tableName &&
          this.keyPattern == other.keyPattern &&
          this.decoder == other.decoder
      case _ ⇒ false
    }
  }
}

/**
  * 批表创建
  */
object CreateBatchTableInfo{

  val parameters:Array[Class[_]] = Array(classOf[String],
    classOf[Seq[SourceField]],
    classOf[Option[String]],
    classOf[String],
    classOf[FieldDecoder],
    classOf[Int],
    classOf[Map[String,String]],
    classOf[Map[String,FieldDecoder]],
    classOf[Map[String,MessageDecoder[AnyRef]]])

  val sourceManager:Map[String,Constructor[_]]=Map(
    "hdfs" → classOf[HDFSCreateBatchTableInfo].getConstructor(parameters:_*),
    "hbase" → classOf[HBaseCreateBatchTableInfo].getConstructor(parameters:_*),
    "mysql" → classOf[MysqlCreateBatchTableInfo].getConstructor(parameters:_*)
  )

  /**
    *
    * @param tableName 表名
    * @param field 字段列表
    * @param key 键值
    * @param required 必填参数
    * @param optional 可选参数
    * @return
    */
  def apply(tableName:String,
            field: Seq[SourceField],
            key:Option[String],
            required: BatchRequired,
            optional:Map[String,String],
            dynamicDecoders:Map[String,FieldDecoder],
            dynamicSourceDataType:Map[String,MessageDecoder[AnyRef]]): CreateBatchTableInfo = {
    sourceManager(required.source).newInstance(tableName,field,key,required.source,
      required.keyDecoder.toDecoder(dynamicDecoders),Int.box(required.parallel),optional,dynamicDecoders,
      dynamicSourceDataType).asInstanceOf[CreateBatchTableInfo]
  }
}

