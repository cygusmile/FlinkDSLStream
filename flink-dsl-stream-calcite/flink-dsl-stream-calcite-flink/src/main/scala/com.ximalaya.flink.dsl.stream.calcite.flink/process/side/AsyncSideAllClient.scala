package com.ximalaya.flink.dsl.stream.calcite.flink.process.side

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Timer, TimerTask}

import com.google.common.collect.Maps
import com.ximalaya.flink.dsl.stream.`type`.{Encoder, SourceField}
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder
import com.ximalaya.flink.dsl.stream.side._
import org.apache.flink.api.common.functions.RuntimeContext

import scala.concurrent.Future
import scala.concurrent.duration.TimeUnit
import java.util.{List ⇒ JList}
/**
  *
  * @author martin.dong
  *
  **/
class AsyncSideAllClient(target:SideLoader,all:All) extends AsyncSideClient {

  @volatile private var data:util.Map[Array[Byte],util.Map[String,Object]] = _
  private var keyEncoder:FieldEncoder = _
  private var tableName:String = _
  private var timer:Timer = _


  private def convertToMilliseconds(ttl:Long,timeUnit: TimeUnit): Long ={
    timeUnit match {
      case TimeUnit.MILLISECONDS ⇒  ttl
      case TimeUnit.SECONDS ⇒ ttl*1000
      case TimeUnit.MINUTES ⇒ ttl*1000*60
      case TimeUnit.HOURS ⇒ ttl*1000*3600
      case TimeUnit.DAYS ⇒ ttl*1000*3600*24
    }
  }

  /**
    * asynchronous query result by key
    *
    * @param value asynchronous result
    * @return
    */
  override def query(value: Object): Future[(String,util.Map[String, Object])] = {
      val result = data.getOrDefault(keyEncoder.encode(value),Maps.newHashMap())
      Future.successful((getTable,result))
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
    target.open(key, sideTable,physicsSchema, castErrorStrategy, physicsSideInfo, runtimeContext)
    data = target.load(all.pattern)
    tableName = sideTable
    keyEncoder = physicsSideInfo.keyEncoder
    timer = new Timer()
    timer.scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = {
        data = target.load(all.pattern)
      }
    },convertToMilliseconds(all.ttl,all.timeUnit),convertToMilliseconds(all.ttl,all.timeUnit))
  }

  /**
    * close method
    */
  override def close(): Unit = {
    target.close()
    timer.cancel()
    keyEncoder.close()
  }

  /**
    * return table name
    *
    * @return table name
    */
  override def getTable: String = {
     tableName
  }
}
