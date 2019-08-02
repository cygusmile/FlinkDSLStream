package com.ximalaya.flink.dsl.stream.calcite

import java.util.concurrent.TimeUnit

import com.ximalaya.flink.dsl.stream.calcite.flink.SqlCompiler
import com.ximalaya.flink.dsl.stream.calcite.flink.context.{CompileContext, QueryRuntimeContext, SideJoinRuntimeContext}
import com.ximalaya.flink.dsl.stream.calcite.flink.process.explode.QueryProcessFunctionFactory
import com.ximalaya.flink.dsl.stream.calcite.flink.process.side.SideJoinProcessFunctionFactory
import org.apache.calcite.sql.SqlIdentifier
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.scala.DataStream
/**
  *
  * @author martin.dong
  *
  *
  **/

package object register {

  implicit class WrapSqlIdentifier(val sqlIdentifier: SqlIdentifier){
      def getNames:java.util.List[String]={
          SqlCompiler.getNames(sqlIdentifier)
      }
  }

  implicit class WrapCompileContext(val compileContext: CompileContext) {

    def register(ds:DataStream[Row],registerNames:Array[String],registerTable:String):Boolean = {
      val registerFields = registerNames.map(e ⇒ Symbol(e))
      registerFields.length match {
        case 1 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0))
        case 2 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1))
        case 3 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2))
        case 4 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3))
        case 5 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4))
        case 6 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5))
        case 7 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5), registerFields(6))
        case 8 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5), registerFields(6), registerFields(7))
        case 9 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8))
        case 10 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8), registerFields(9))
        case 11 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8), registerFields(9), registerFields(10))
        case 12 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8), registerFields(9), registerFields(10), registerFields(11))
        case 13 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8), registerFields(9), registerFields(10), registerFields(11), registerFields(12))
        case 14 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8), registerFields(9), registerFields(10), registerFields(11), registerFields(12),
          registerFields(13))
        case 15 ⇒ compileContext.getTableEnvironment.registerDataStream(registerTable, ds, registerFields(0),
          registerFields(1), registerFields(2), registerFields(3), registerFields(4), registerFields(5),
          registerFields(6), registerFields(7), registerFields(8), registerFields(9), registerFields(10), registerFields(11), registerFields(12),
          registerFields(13), registerFields(14))
      }
      true
    }

    def register(sql: String): Boolean = {
      try {
        val context = SqlCompiler.compile(sql, compileContext)
        context match {
          case queryRuntimeContext: QueryRuntimeContext ⇒
            val processFunction = QueryProcessFunctionFactory.createQueryProcessFunction(queryRuntimeContext)
            val ds = compileContext.getTableEnvironment.
              sqlQuery(s"select * from ${queryRuntimeContext.getSourceTable}").
              toAppendStream[Row].process(processFunction)(new RowTypeInfo(queryRuntimeContext.getRegisterTypes.map(e ⇒ TypeInformation.of(e)): _*))
           register(ds,queryRuntimeContext.getRegisterNames,queryRuntimeContext.getRegisterTable)
          case sideJoinRuntimeContext:SideJoinRuntimeContext ⇒
              val asyncFunc = SideJoinProcessFunctionFactory.createSideProcessFunction(sideJoinRuntimeContext,compileContext)
              val ds = compileContext.getTableEnvironment.sqlQuery(s"select * from ${sideJoinRuntimeContext.sourceTable}")
                .toAppendStream[Row].javaStream
              val asyncDs = new DataStream(AsyncDataStream.
                orderedWait(ds,asyncFunc,1000L,TimeUnit.MICROSECONDS,10).
                returns(new RowTypeInfo(sideJoinRuntimeContext.registerTypes.map(e ⇒ TypeInformation.of(e)): _*))).filter(_!=null)
            register(asyncDs,sideJoinRuntimeContext.registerNames,sideJoinRuntimeContext.registerTable)
        }
      } catch {
        case e: Exception ⇒ throw new RuntimeException(e)
      }
    }
  }
}