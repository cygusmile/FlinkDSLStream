package com.ximalaya.flink.dsl.stream.calcite.flink.process.side

import com.ximalaya.flink.dsl.stream.calcite.flink.context.{CompileContext, SideJoinRuntimeContext}
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, RichAsyncFunction}
import org.apache.flink.types.Row

/**
  *
  * @author martin.dong
  *
  **/

object SideJoinProcessFunctionFactory {

  def createSideProcessFunction(sideJoinRuntimeContext: SideJoinRuntimeContext,
                                compileContext: CompileContext):AsyncFunction[Row,Row]={

      val sideTableInfoMap = sideJoinRuntimeContext.conditions.
        map(condition⇒condition.sideTable).
        map(table⇒(table,compileContext.querySideSchema(table).get())).toMap

      new SideProcessFunction(sideJoinRuntimeContext.sourceNames,
        sideJoinRuntimeContext.sourceTable,
        sideJoinRuntimeContext.selectEvaluations,
        sideJoinRuntimeContext.whereEvaluation,
        sideJoinRuntimeContext.conditions,
        compileContext.getSideCatalog,
        sideTableInfoMap)
  }
}
