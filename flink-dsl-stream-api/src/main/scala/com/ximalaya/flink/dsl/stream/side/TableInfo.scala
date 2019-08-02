package com.ximalaya.flink.dsl.stream.side

import java.util.{List ⇒ JList, Map ⇒ JMap}

import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.{FieldEncoder, NoneFieldEncoder, StringFieldEncoder}
import com.ximalaya.flink.dsl.stream.side.FileType.FieldTypeValue

import scala.concurrent.duration.TimeUnit
/**
  *
  * @author martin.dong
  *
  **/
/**
  * 维表缓存策略
  */
sealed trait CacheStrategy extends Serializable

/**
  * 不缓存维表数据
  */
object Nothing extends CacheStrategy{
  override def toString: String = "Nothing"
}

/**
  * 缓存全部维表数据
  * @param pattern 键匹配模式
  * @param ttl 更新时间间隔
  * @param timeUnit 更新时间单位
  */
case class All(pattern:String,
               ttl:Long,
               timeUnit: TimeUnit) extends CacheStrategy{
  override def toString: String = s"All(pattern = $pattern ttl = $ttl timeUnit = $timeUnit)"
}

/**
  * LRU缓存维表数据
  * @param pattern 键匹配模型
  * @param cacheSize 缓存的数据量
  * @param ttl 数据失效时间
  * @param timeUnit 数据失效时间单位
  */
case class Lru(pattern:String,
               cacheSize:Int,
               ttl:Long,
               timeUnit: TimeUnit) extends CacheStrategy{
  override def toString = s"Lru(pattern = $pattern cacheSize = $cacheSize ttl = $ttl timeUnit = $timeUnit)"
}

/**
  * 流表 批表或维表数据转换错误策略
  */
sealed trait CastErrorStrategy extends Serializable

/**
  * 忽略
  */
object Ignore extends CastErrorStrategy{
  override def toString: String = "ignore"
}

/**
  * 停止
  */
object Stop extends CastErrorStrategy{
  override def toString: String = "stop"
}

object CastErrorStrategy{
  def apply(name:String):CastErrorStrategy={
    name match{
      case "ignore"⇒Ignore
      case "stop"⇒Stop
    }
  }
}

/**
  * 数据源信息
  */
sealed trait PhysicsSideInfo extends Serializable{
    val name:String
    val keyEncoder:FieldEncoder
}

object PhysicsSideInfo{

    def apply(name:String,
              key:String,
              sideProps:Map[String,String],
              physicsSchema: Seq[SourceField],
              keyEncoder:Option[FieldEncoder],
              decoder:Option[FieldDecoder]):PhysicsSideInfo={
        name match {
          case "hbase" ⇒
            //check fields
            physicsSchema.foreach(field⇒
              require(field.getPaths.isDefined && field.getPaths.get.length==1,"Parse Error! hbase field definition error ( columnFamily.column [ as alias ] )")
            )
            //check parameters
            require(sideProps.contains("zookeeper") &&
              sideProps.contains("tableName") &&
              sideProps.contains("decoder"),"Parse error! hbase side props must contain zookeeper, tableName and decoder")
            HBaseSideInfo(sideProps("tableName"),sideProps("zookeeper"),keyEncoder.get,decoder.get)
          case "mysql" ⇒
            //check parameters
            require(checkPrimaryKeyLegal)
            def checkPrimaryKeyLegal:Boolean = {
              physicsSchema.exists(field⇒field.getAliasName.getOrElse(field.getFieldName)==key)
            }
            //check parameters
            require(sideProps.contains("jdbcUrl") &&
              sideProps.contains("dbName") &&
              sideProps.contains("tableName") &&
              sideProps.contains("username") &&
              sideProps.contains("password"))
            MysqlSideInfo(sideProps("tableName"),sideProps("dbName"),sideProps("jdbcUrl"),sideProps("username"),sideProps("password"))
          case "redis" ⇒
            //check parameters
            require(sideProps.contains("dataType") && sideProps("dataType") == "hash" || sideProps("dataType") == "string")
            require(sideProps.contains("url") &&
              sideProps.contains("password") &&
              sideProps.contains("username") &&
              sideProps.contains("database") &&
              sideProps.contains("decoder"))
            RedisSideInfo(RedisDataType(sideProps("dataType")),sideProps("url"),sideProps("password"),
              sideProps("username"),sideProps("database").toInt,keyEncoder.get,decoder.get)
          case "hdfs" ⇒
            //check parameters
            require(sideProps.contains("path") &&
              sideProps.contains("fsDefaultName") &&
              sideProps.contains("zip") &&
              sideProps.contains("dataType"))
            HDFSSideInfo(sideProps("path"),sideProps("fsDefaultName"),ZipStrategy(sideProps("zip")),FileType(sideProps("dataType")))
        }
    }
}

/**
  * HBase数据源信息
  * @param tableName HBase表名
  * @param zookeeper Zookeeper连接信息
  * @param keyEncoder 键编码器
  * @param decoder 值编码器
  */
case class HBaseSideInfo(tableName:String,
                         zookeeper:String,
                         keyEncoder:FieldEncoder,
                         decoder:FieldDecoder) extends PhysicsSideInfo{
  override val name: String = "hbase"

  override def toString: String = s"HBaseSideInfo(tableName = $tableName " +
    s"zookeeper = $zookeeper keyEncoder = $keyEncoder decoder = $decoder)"

}

/**
  * Mysql数据源信息
  * @param tableName Mysql表名
  * @param dbName Mysql数据库名
  * @param jdbcUrl Mysql连接信息
  * @param username Mysql连接用户名
  * @param password Mysql连接密码
  */
case class MysqlSideInfo(tableName:String,
                         dbName:String,
                         jdbcUrl:String,
                         username:String,
                         password:String) extends PhysicsSideInfo{
  override val name: String = "mysql"

  override val keyEncoder: FieldEncoder = new NoneFieldEncoder

  override def toString: String = s"MysqlSideInfo(tableName = $tableName dbName = $dbName " +
    s"jdbcUrl = $jdbcUrl username = $username password = $password)"
}


object RedisDataType extends Enumeration{
    val Hash,String = Value
    def apply(dataType:String):RedisDataType.Value={
        dataType match {
          case "hash" ⇒ Hash
          case "string" ⇒ String
        }
    }
}

/**
  * Redis数据源信息
  * @param dataType 数据源类型
  * @param url Redis连接信息
  * @param password Redis连接用户名
  * @param username Redis连接密码
  * @param database Redis连接数据库
  * @param keyEncoder Redis键编码器
  * @param decoder Redis值编码器
  */
case class RedisSideInfo(dataType: RedisDataType.Value,
                 url:String,
                 password:String,
                 username:String,
                 database:Int,
                 keyEncoder:FieldEncoder,
                 decoder:FieldDecoder) extends PhysicsSideInfo{
  override val name: String = s"redis"
  override def toString: String = s"RedisSideInfo(dataType = $dataType url = $url password = " +
    s"$password username = $username database = $database keyEncoder = $keyEncoder decoder = $decoder)"
}

object ZipStrategy extends Enumeration{
    val zip,gzip,none = Value
    def apply(name:String):ZipStrategy.Value={
        name match {
          case "zip" ⇒ zip
          case "gzip" ⇒ gzip
          case "none" ⇒ none
        }
    }
}


object FileType extends Enumeration{
  case class FieldTypeValue(override val id:Int,name:String,keyEncoder: FieldEncoder) extends Val(id,name)
    val Json = FieldTypeValue(1,"json",new StringFieldEncoder)
    val Csv = FieldTypeValue(2,"csv",new StringFieldEncoder)
    def apply(name:String):FileType.Value = {
        name match {
          case "json" ⇒ Json
          case "csv" ⇒ Csv
        }
    }
}

/**
  * HDFS数据源信息
  * @param path HDFS路径名
  * @param fsDefaultName HDFS连接信息
  * @param zipStrategy HDFS文件压缩类型
  * @param dataType HDFS文件类型
  */
case class HDFSSideInfo(path:String,
                        fsDefaultName:String,
                        zipStrategy: ZipStrategy.Value,
                        dataType:FileType.Value) extends PhysicsSideInfo{
  override val name: String = s"hdfs$dataType"
  override val keyEncoder: FieldEncoder = dataType.asInstanceOf[FieldTypeValue].keyEncoder

  override def toString: String = s"HDFSSideInfo(path = $path fsDefaultName = $fsDefaultName" +
    s" zipStrategy = $zipStrategy dataType = $dataType)"
}

/**
  * 维表信息
  * @param tableName 维表
  * @param physicsSideInfo 维表底层数据源信息
  * @param logicSchema 维表逻辑视图
  * @param key 维表主键字段
  * @param cacheStrategy 维表缓存策略
  * @param physicsSchema 维表物理视图
  * @param castErrorStrategy 维表数据类型转换错误
  */
case class SideTableInfo(tableName:String,
                         physicsSideInfo: PhysicsSideInfo,
                         logicSchema:JMap[String,Class[_]],
                         key:String,
                         cacheStrategy: CacheStrategy,
                         physicsSchema:JList[SourceField],
                         castErrorStrategy: CastErrorStrategy) extends Serializable
