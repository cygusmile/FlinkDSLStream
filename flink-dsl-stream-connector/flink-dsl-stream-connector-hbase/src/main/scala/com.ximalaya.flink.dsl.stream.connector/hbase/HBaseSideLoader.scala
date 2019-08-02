package com.ximalaya.flink.dsl.stream.connector.hbase

import java.util

import com.google.common.collect.Maps
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.side.{CastErrorStrategy, HBaseSideInfo, PhysicsSideInfo, SideLoader}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  *
  * @author martin.dong
  *
  **/

class HBaseSideLoader extends SideLoader {


  private var connection: Connection = _
  private var decoder: FieldDecoder = _
  private var tableName: String = _
  private var physicsSchema: List[(String, String, Option[String], Option[AnyRef],FieldType)] = _

  /**
    * 加载维表数据方法
    *
    * @return 被加载的数据
    */
  override def load(keyPattern: String): util.Map[Array[Byte], util.Map[String, Object]] = {
    val data = Maps.newHashMap[Array[Byte], util.Map[String, Object]]()
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    import scala.collection.convert.wrapAsScala._
    val iterator = table.getScanner(scan).iterator()
    iterator.foreach(result ⇒ {
      val value = Maps.newHashMap[String, Object]()
      if (result != null) {
        physicsSchema.foreach {
          case (columnFamily, column, alias,default,fieldType) ⇒
            val cell = result.getColumnLatestCell(columnFamily.getBytes, column.getBytes())
            if (cell != null) {
              val columnValue = decoder.decode(cell.getValueArray, fieldType)
              value.put(alias.getOrElse(column), columnValue)
            }else if(default.isDefined){
              value.put(alias.getOrElse(column),default.get)
            }
        }
      }
      data.put(result.getRow, value)
    })
    data
  }

  /**
    * 初始化方法 比如创建数据库连接等
    *
    * @param key               key field name 维表主键字段名
    * @param sideTable         side table name 维表名
    * @param physicsSchema     physics schema 维表物理层schema信息
    * @param castErrorStrategy cast error strategy 维表数据转换错误处理策略 忽略或报错
    * @param physicsSideInfo   physics side info 维表物理层数据源连接信息
    * @param runtimeContext    runtime context Flink运行时对象
    */
  override def open(key: String, sideTable: String, physicsSchema: util.List[SourceField],
                    castErrorStrategy: CastErrorStrategy, physicsSideInfo: PhysicsSideInfo,
                    runtimeContext: RuntimeContext): Unit = {
    val hBaseSideInfo = physicsSideInfo.asInstanceOf[HBaseSideInfo]
    var conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hBaseSideInfo.zookeeper)
    connection = ConnectionFactory.createConnection(conf)

    decoder = hBaseSideInfo.decoder
    tableName = hBaseSideInfo.tableName
    import scala.collection.convert.wrapAsScala._
    this.physicsSchema = physicsSchema.map(field ⇒ {
      val columnFamily = field.getPaths.get.head
      val column = field.getFieldName
      (columnFamily, column, field.getAliasName, field.getDefaultValue,field.getFieldType)
    }).toList
  }

  /**
    * 关闭方法 比如关闭数据库连接等
    */
  override def close(): Unit = {
    connection.close()
    decoder.close()
  }
}
object HBaseSideLoader{
  def main(args: Array[String]): Unit = {
    var conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "storage6.test.lan:2108,storage7.test.lan:2108,storage8.test.lan:2108")
    var connection = ConnectionFactory.createConnection(conf)

    connection.getTable(TableName.valueOf("xx"))


  }
}
