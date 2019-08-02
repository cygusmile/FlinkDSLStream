package com.ximalaya.flink.dsl.stream.calcite.flink

import java.util

import com.google.common.collect.Maps
import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.side.{AsyncSideClient, CastErrorStrategy, PhysicsSideInfo}
import org.apache.flink.api.common.functions.RuntimeContext

import scala.concurrent.Future

/**
  *
  * @author martin.dong
  *
  **/

class TestAsyncSideClient1 extends AsyncSideClient {

  private var sideTable: String = _
  private var data: util.Map[Object, util.Map[String, Object]] = Maps.newHashMap()

  /**
    * 通过主键异步访问维表数据
    *
    * @param value 主键值
    * @return 异步查询结果 结果中包含 维表名和查询结果
    */
  override def query(value: Object): Future[(String, util.Map[String, Object])] = {
    Future.successful((sideTable, data.get(300L)))
  }

  /**
    * 返回被查询维表名
    *
    * @return 维表名
    */
  override def getTable: String = {
    sideTable
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
    this.sideTable = sideTable

    val value1 = Maps.newHashMap[String, Object]()
    value1.put("name", "mark")
    value1.put("salary",Double.box(1222.2))
    data.put(Long.box(300), value1)

    val value2 = Maps.newHashMap[String, Object]()
    value2.put("name", "mmm")
    value2.put("salary", Double.box(1223.2))

    data.put(Long.box(301), value2)
  }

  /**
    * 关闭方法 比如关闭数据库连接等
    */
  override def close(): Unit = {

  }
}
