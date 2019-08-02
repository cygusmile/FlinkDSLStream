package com.ximalaya.flink.dsl.stream.side
import java.util.{Map ⇒ JMap}

import com.ximalaya.flink.dsl.stream.annotation.Internal
/**
  *
  * @author martin.dong
  *
  **/
/**
  * 维表数据加载访问句柄
  */
@Internal
trait SideLoader extends SideLifeCycle{

  /**
    * 加载维表数据方法
    * @param keyPattern 键范围
    * @return 被加载的数据
    */
  def load(keyPattern:String):JMap[Array[Byte],JMap[String,Object]]
}
