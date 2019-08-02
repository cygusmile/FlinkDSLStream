package com.ximalaya.flink.dsl.stream.side


import java.util.{Map ⇒ JMap}

import com.ximalaya.flink.dsl.stream.annotation.Internal

import scala.concurrent.Future
/**
  *
  * @author martin.dong
  *
  * 维表数据异步访问句柄
  **/
@Internal
trait AsyncSideClient extends SideLifeCycle{

  /**
    *通过主键异步访问维表数据
    * @param value 主键值
    * @return 异步查询结果 结果中包含 维表名和查询结果
    */
  def query(value:Object):Future[(String,JMap[String,Object])]

  /**
    * 返回被查询维表名
    * @return 维表名
    */
  def getTable:String
}
