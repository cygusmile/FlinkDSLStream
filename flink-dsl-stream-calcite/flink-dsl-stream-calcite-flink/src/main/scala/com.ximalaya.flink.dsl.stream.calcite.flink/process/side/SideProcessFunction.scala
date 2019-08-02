package com.ximalaya.flink.dsl.stream.calcite.flink.process.side

import com.google.common.collect.{Lists, Maps}
import com.ximalaya.flink.dsl.stream.side._
import com.ximalaya.flink.dsl.stream.calcite.flink.context.{ConditionInfo, SideCatalog}
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.{Constant, Evaluation}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.types.Row
import java.util.{Map ⇒ JMap}
import scala.concurrent.Future
/**
  *
  * @author martin.dong
  *
  **/

class Result(var discard:Boolean,var result:JMap[String,Object])

class SideProcessFunction(fields:Array[String],
                          streamTable:String,
                          queries:List[Evaluation],
                          where:Option[Evaluation],
                          conditions:List[ConditionInfo],
                          sideCatalog: SideCatalog,
                          sideTableInfoMap: Map[String,SideTableInfo]) extends RichAsyncFunction[Row,Row] {
  @transient var mustJoinFlags:Map[String,Boolean] = _
  @transient var sideClients:Map[String,AsyncSideClient] = _

  protected def convert(row: Row, fields: Array[String]): JMap[String, AnyRef] = {
    val result:java.util.Map[String,AnyRef] = Maps.newHashMap()
    fields.zipWithIndex.foreach{
      case (_,index) ⇒
        result.put(fields(index),row.getField(index))
    }
    result
  }

  private def wrapTable(value:JMap[String,Object],table:String):JMap[String,Object]= {
    val wrapResult = Maps.newHashMap[String, Object]()
    import scala.collection.convert.wrapAsScala._
    value.foreach({
      case (k, v) ⇒ wrapResult.put(table+"$"+k, v)
    })
    wrapResult
  }

  override def asyncInvoke(input: Row, resultFuture: ResultFuture[Row]): Unit = {
      val wrapResult = wrapTable(convert(input,fields),streamTable)

      val filterResult = where.map(e⇒e.eager(wrapResult))

      if(filterResult.isDefined
        && filterResult.get.isInstanceOf[Constant]) {
        val filter = filterResult.get.asInstanceOf[Constant].eval(null).asInstanceOf[Boolean]
        if (filter) {
          resultFuture.complete(null)
          return
        }
      }

      val futures = conditions.map(condition⇒{
          val key = condition.streamEvaluation.eval(wrapResult)
          sideClients(condition.sideTable).query(key)
      })
      import scala.concurrent.ExecutionContext.Implicits.global
      val combineFuture = Future.fold(futures)(new Result(false,wrapResult))((remainResult, newResult)⇒ {
        if (remainResult.discard) {
          remainResult
        } else {
          val (sideTable, sideResult) = newResult
          val wrapSideResult = wrapTable(sideResult, sideTable)
          if (wrapSideResult.isEmpty && mustJoinFlags(sideTable)) {
            remainResult.discard = true
            remainResult
          } else {
            remainResult.result.putAll(wrapSideResult)
            remainResult
          }
        }
      })
      combineFuture.onSuccess{
        case v⇒
            if(v.discard){
                resultFuture.complete(null)
            }else{
                if(filterResult.isDefined){
                  val filter = filterResult.get.eval(v.result).asInstanceOf[Boolean]
                  if(!filter){
                    resultFuture.complete(null)
                  }else{
                    resultFuture.complete(Lists.newArrayList(Row.of(queries.map(query ⇒ query.eval(v.result)): _*)))
                  }
                }else {
                  resultFuture.complete(Lists.newArrayList(Row.of(queries.map(query ⇒ query.eval(v.result)): _*)))
                }
            }
      }
      combineFuture.onFailure{
        case e⇒
            resultFuture.completeExceptionally(e)
      }

  }

  private def initClient(sideTableInfo: SideTableInfo,runtimeContext: RuntimeContext):AsyncSideClient={
     sideCatalog.sideClients.get(sideTableInfo.physicsSideInfo.name).newInstance()
  }
  private def initLoader(sideTableInfo: SideTableInfo,runtimeContext: RuntimeContext):SideLoader = {
      val loader = sideCatalog.sideLoaders.get(sideTableInfo.physicsSideInfo.name).newInstance()
      loader.open(sideTableInfo.key,
        sideTableInfo.tableName,
        sideTableInfo.physicsSchema,
        sideTableInfo.castErrorStrategy,
        sideTableInfo.physicsSideInfo,
        runtimeContext)
      loader
  }

  override def open(parameters: Configuration): Unit = {
    queries.foreach(_.open(getRuntimeContext))
    conditions.foreach(_.streamEvaluation.open(getRuntimeContext))
    where.foreach(e⇒e.open(getRuntimeContext))

    sideClients = sideTableInfoMap.toList.map{
      case (tableName,sideTableInfo) ⇒
          sideTableInfo.cacheStrategy match {
            case Nothing ⇒
              val client = initClient(sideTableInfo,getRuntimeContext)
              client.open(sideTableInfo.key,
                sideTableInfo.tableName,
                sideTableInfo.physicsSchema,
                sideTableInfo.castErrorStrategy,
                sideTableInfo.physicsSideInfo,
                getRuntimeContext)
              (tableName,client)
            case lru:Lru ⇒
              val client = new AsyncSideLruCacheClient(initClient(sideTableInfo,getRuntimeContext),lru)
              client.open(sideTableInfo.key,
                sideTableInfo.tableName,
                sideTableInfo.physicsSchema,
                sideTableInfo.castErrorStrategy,
                sideTableInfo.physicsSideInfo,
                getRuntimeContext)
              (tableName,client)
            case all:All ⇒
              val client = new AsyncSideAllClient(initLoader(sideTableInfo,getRuntimeContext),all)
              client.open(sideTableInfo.key,
                sideTableInfo.tableName,
                sideTableInfo.physicsSchema,
                sideTableInfo.castErrorStrategy,
                sideTableInfo.physicsSideInfo,
                getRuntimeContext)
              (tableName,client)
          }
    }.toMap
    mustJoinFlags = conditions.map(condition⇒(condition.sideTable,condition.mustJoin)).toMap
  }

  override def close(): Unit = {
      queries.foreach(_.close())
      conditions.foreach(_.streamEvaluation.close())
      where.foreach(e⇒e.close())
      sideClients.values.foreach(_.close())
  }

  override def timeout(input: Row, resultFuture: ResultFuture[Row]): Unit = {}
}
