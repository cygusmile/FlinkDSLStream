package com.ximalaya.flink.dsl.stream.calcite.flink.process.side

import java.util
import java.util.concurrent.ConcurrentMap

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import com.ximalaya.flink.dsl.stream.`type`.SourceField
import com.ximalaya.flink.dsl.stream.side.{AsyncSideClient, CastErrorStrategy, Lru, PhysicsSideInfo}
import org.apache.flink.api.common.functions.RuntimeContext
import java.util.{Map ⇒ JMap}
import scala.concurrent.Future
import java.util.{List ⇒ JList}

/**
  *
  * @author martin.dong
  *
  **/

class AsyncSideLruCacheClient(target:AsyncSideClient,lru:Lru) extends AsyncSideClient {

  private var cache:ConcurrentMap[Object,util.Map[String,Object]] = _
  /**
    * asynchronous query result by key
    *
    * @param value asynchronous result
    * @return
    */
  override def query(value: Object): Future[(String,JMap[String,Object])] = {
      val cacheResult = cache.get(value)
      if(cacheResult == null){
          val future = target.query(value)
          import scala.concurrent.ExecutionContext.Implicits.global
          future.onSuccess{
            case e⇒cache.put(value,e._2)
          }
          future
      }else{
          Future.successful((getTable,cacheResult))
      }
  }


  /**
    * initialization method
    *
    * @param key               key field name
    * @param physicsSchema     physics schema
    * @param castErrorStrategy cast error strategy
    * @param physicsSideInfo   physics side info
    * @param runtimeContext    runtime context
    */
  override def open(key: String,
                    sideTable:String,
                    physicsSchema: JList[SourceField],
                    castErrorStrategy: CastErrorStrategy,
                    physicsSideInfo: PhysicsSideInfo,
                    runtimeContext: RuntimeContext): Unit = {
    target.open(key,sideTable, physicsSchema, castErrorStrategy, physicsSideInfo, runtimeContext)
    cache = new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity(lru.cacheSize).build()
  }

  /**
    * close method
    */
  override def close(): Unit = {
    target.close()
    cache.clear()
  }

  /**
    * return table name
    *
    * @return table name
    */
  override def getTable: String = target.getTable
}
