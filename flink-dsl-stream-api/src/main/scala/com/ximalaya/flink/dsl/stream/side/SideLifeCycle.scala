package com.ximalaya.flink.dsl.stream.side

import java.io.IOException
import java.util.{List ⇒ JList}

import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.annotation.Internal
import org.apache.flink.api.common.functions.RuntimeContext
/**
  *
  * @author martin.dong
  *
  * 维表数据访问句柄的生命周期接口
  **/
@Internal
trait SideLifeCycle {

  /**
    * 初始化方法 比如创建数据库连接等
    * @param key key field name 维表主键字段名
    * @param sideTable side table name 维表名
    * @param physicsSchema physics schema 维表物理层schema信息
    * @param castErrorStrategy cast error strategy 维表数据转换错误处理策略 忽略或报错
    * @param physicsSideInfo physics side info 维表物理层数据源连接信息
    * @param runtimeContext runtime context Flink运行时对象
    */
  @throws[IOException]
  def open(key:String,
           sideTable:String,
           physicsSchema:JList[SourceField],
           castErrorStrategy:CastErrorStrategy,
           physicsSideInfo: PhysicsSideInfo,
           runtimeContext: RuntimeContext)

  /**
    *关闭方法 比如关闭数据库连接等
    */
  @throws[IOException]
  def close()
}
