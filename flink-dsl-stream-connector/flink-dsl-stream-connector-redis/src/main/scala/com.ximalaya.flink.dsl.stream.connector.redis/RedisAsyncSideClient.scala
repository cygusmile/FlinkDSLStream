package com.ximalaya.flink.dsl.stream.connector.redis

import java.util

import com.google.common.collect.Maps
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.side._
import org.apache.flink.api.common.functions.RuntimeContext
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.concurrent.Future


/**
  *
  * @author martin.dong
  *
  **/

class RedisAsyncSideClient extends AsyncSideClient {

  private var sideTable: String = _
  private var pool: JedisPool = _
  private var keyEncoder: FieldEncoder = _
  private var decoder: FieldDecoder = _
  private var dataType: RedisDataType.Value = _

  private var physicsSchema: List[(String, Option[String], Option[AnyRef], FieldType)] = _

  /**
    * 通过主键异步访问维表数据
    *
    * @param value 主键值
    * @return 异步查询结果 结果中包含 维表名和查询结果
    */
  override def query(value: Object): Future[(String, util.Map[String, Object])] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    var client: Jedis = null
    Future {
      val map = Maps.newHashMap[String, Object]()
      try {
        client = pool.getResource
        dataType match {
          case RedisDataType.Hash ⇒
            import scala.collection.convert.wrapAsScala._
            val fields = physicsSchema.map(_._1.getBytes)
            val result = client.hmget(keyEncoder.encode(value), fields: _*)
            physicsSchema.zip(result).foreach {
              case ((field, alias, default, fieldType), keyValue) ⇒
                if (keyValue != null) {
                  map.put(field, decoder.decode(keyValue, fieldType))
                } else if (default.isDefined) {
                  map.put(field, default.get)
                }
            }
          case RedisDataType.String ⇒
            val (field, alias, default, fieldType) = physicsSchema.head
            val keyValue = client.get(keyEncoder.encode(map))
            if (keyValue != null) {
              map.put(field, decoder.decode(keyValue, fieldType))
            } else if (default.isDefined) {
              map.put(field, default.get)
            }
        }
        (sideTable, map)
      } finally {
        if (client != null) {
          client.close()
        }
      }
    }
  }

  /**
    * 返回被查询维表名
    *
    * @return 维表名
    */
  override def getTable: String = sideTable

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
    this.sideTable = sideTable
    val redisSideInfo = physicsSideInfo.asInstanceOf[RedisSideInfo]
    val config = new JedisPoolConfig
    pool = new JedisPool(config, redisSideInfo.url.split(":")(0), redisSideInfo.url.split(":")(1).toInt,
      200, redisSideInfo.password, redisSideInfo.database)
    dataType = redisSideInfo.dataType
    keyEncoder = redisSideInfo.keyEncoder
    decoder = redisSideInfo.decoder

    keyEncoder.open(runtimeContext)
    decoder.open(runtimeContext)

    import scala.collection.convert.wrapAsScala._
    this.physicsSchema = physicsSchema.map(field ⇒ {
      (field.getFieldName, field.getAliasName, field.getDefaultValue, field.getFieldType)
    }).toList
  }

  /**
    * 关闭方法 比如关闭数据库连接等
    */
  override def close(): Unit = {
    pool.close()
    keyEncoder.close()
    decoder.close()
  }
}
