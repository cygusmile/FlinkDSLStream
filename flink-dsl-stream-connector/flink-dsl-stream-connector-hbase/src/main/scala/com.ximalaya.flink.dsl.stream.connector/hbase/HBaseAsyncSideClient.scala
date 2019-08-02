package com.ximalaya.flink.dsl.stream.connector.hbase

import java.util
import java.util.{List ⇒ JList}

import com.google.common.collect.Maps
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.side.{AsyncSideClient, CastErrorStrategy, HBaseSideInfo, PhysicsSideInfo}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.concurrent.Future

/**
  *
  * @author martin.dong
  *
  **/
class HBaseAsyncSideClient extends AsyncSideClient {


  private var connection: Connection = _
  private var keyEncoder: FieldEncoder = _
  private var decoder: FieldDecoder = _
  private var tableName: String = _
  private var sideTableName: String = _
  private var physicsSchema: List[(String, String, Option[String], Option[AnyRef], FieldType)] = _


  /**
    * asynchronous query result by key
    *
    * @param value asynchronous result
    * @return
    */
  override def query(value: Object): Future[(String, util.Map[String, Object])] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      val map = Maps.newHashMap[String, Object]()
      val result = connection.getTable(TableName.valueOf(tableName)).get(new Get(keyEncoder.encode(value)))
      physicsSchema.foreach({
        case (columnFamily, column, alias, default, fieldType) ⇒
          val cell = result.getColumnLatestCell(columnFamily.getBytes, column.getBytes)
          if (cell != null) {
            val columnValue = decoder.decode(cell.getValue, fieldType)
            map.put(alias.getOrElse(column), columnValue)
          } else if (default.isDefined) {
            map.put(alias.getOrElse(column), default.get)
          }
      })
      (sideTableName,map)
    }
  }

  /**
    * return table name
    *
    * @return table name
    */
  override def getTable: String = sideTableName

  /**
    * initialization method
    *
    * @param key               key field name
    * @param sideTable         side table name
    * @param physicsSchema     physics schema
    * @param castErrorStrategy cast error strategy
    * @param physicsSideInfo   physics side info
    * @param runtimeContext    runtime context
    */
  override def open(key: String, sideTable: String, physicsSchema: JList[SourceField], castErrorStrategy: CastErrorStrategy,
                    physicsSideInfo: PhysicsSideInfo, runtimeContext: RuntimeContext): Unit = {
    val hBaseSideInfo = physicsSideInfo.asInstanceOf[HBaseSideInfo]
    var conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hBaseSideInfo.zookeeper)
    connection = ConnectionFactory.createConnection(conf)
    decoder = hBaseSideInfo.decoder
    keyEncoder = hBaseSideInfo.keyEncoder
    tableName = hBaseSideInfo.tableName
    this.sideTableName = sideTable

    import scala.collection.convert.wrapAsScala._
    this.physicsSchema = physicsSchema.map(field ⇒ {
      val columnFamily = field.getPaths.get.head
      val column = field.getFieldName
      (columnFamily, column, field.getAliasName, field.getDefaultValue, field.getFieldType)
    }).toList
  }

  /**
    * close method
    */
  override def close(): Unit = {
    connection.close()
    decoder.close()
    keyEncoder.close()
  }
}
