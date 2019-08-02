package com.ximalaya.flink.dsl.stream.calcite.flink.context

import com.ximalaya.flink.dsl.stream.side.{AsyncSideClient, SideLoader}
import java.util.{Map⇒JMap}
/**
  *
  * @author martin.dong
  *
  *
  **/

case class SideCatalog(sideClients:JMap[String,Class[_<: AsyncSideClient]],
                       sideLoaders:JMap[String,Class[_<:SideLoader]]) extends Serializable
