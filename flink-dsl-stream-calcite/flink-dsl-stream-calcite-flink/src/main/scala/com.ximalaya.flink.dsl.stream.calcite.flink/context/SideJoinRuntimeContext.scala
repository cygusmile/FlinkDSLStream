package com.ximalaya.flink.dsl.stream.calcite.flink.context

import com.google.common.collect.{ImmutableList, Sets}
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSideJoinSqlSelect
import com.ximalaya.flink.dsl.stream.calcite.flink.SqlCompiler
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation
import org.apache.calcite.sql._

import scala.collection.mutable.ArrayBuffer


case class JoinContext(streamTable: String, sideTables: java.util.Set[String], tableRealNameMap: java.util.Map[String, String])

/**
  *
  * @author martin.dong
  *
  **/
/**
  * 维流表连接信息
  *
  * @param sideTableField   维表连接字段名
  * @param sideTable        维表名
  * @param streamEvaluation 流表连接字段执行代码
  * @param mustJoin         是否一定连接
  */
case class ConditionInfo(sideTableField: String,
                         sideTable: String,
                         streamEvaluation: Evaluation,
                         mustJoin: Boolean) extends Serializable

/**
  * 维流表连接上下文
  *
  * @param sourceTable       源表名字
  * @param selectEvaluations 查询执行表达式列表
  * @param conditions        维流表连接信息列表
  * @param whereEvaluation   过滤执行表达式列表
  * @param registerTable     注册表名字
  * @param sourceNames       源表字段列表
  * @param registerNames     注册表字段列表
  * @param registerTypes     注册表字段类型列表
  */
case class SideJoinRuntimeContext(sourceTable: String,
                                  selectEvaluations: List[Evaluation],
                                  conditions: List[ConditionInfo],
                                  whereEvaluation: Option[Evaluation],
                                  registerTable: String,
                                  sourceNames: Array[String],
                                  registerNames: Array[String],
                                  registerTypes: Array[Class[_]]) extends SqlRuntimeContext

object SideJoinRuntimeContext {

  import scala.collection.convert.wrapAsJava._
  import scala.collection.convert.wrapAsScala._

  /**
    * 维流连接表名信息 (tableName,tableAliasName,mustJoin) -> (维表或流表名,维表或流表别名,是否必须连接)
    */
  type JoinInfo = (String, Option[String], Boolean)

  /**
    * 维流表连接表名和条件信息 (tableName,conditions,tableRealNameMap,sideTables) -> (流表,维流表连接条件,表别名和表名映射关系,维表表名集合)
    */
  type JoinContext = (String, List[ConditionInfo], Map[String, String], Set[String])

  /**
    * 条件字段表达式 (tableName,fieldName,isSideTable) -> (表,字段,是否为维表字段)
    */
  type ConditionIdentifier = (String, String, Boolean)



  import com.ximalaya.flink.dsl.stream.calcite.register._

  private def parseJoin(sqlBasicCall: SqlBasicCall, mustJoin: Boolean): JoinInfo = {
    require(sqlBasicCall.getOperator.isInstanceOf[SqlAsOperator], "sqlBasicCall must be SqlAsOperator: " + sqlBasicCall)
    val operands = sqlBasicCall.getOperands
    val tableName = operands(0).asInstanceOf[SqlIdentifier].getNames.get(0)
    val tableAliasName = Option.apply(operands(1).asInstanceOf[SqlIdentifier].getNames.get(0))
    (tableName, tableAliasName, mustJoin)
  }

  private def parseJoin(sqlIdentifier: SqlIdentifier, mustJoin: Boolean): JoinInfo = {
    val tableName = sqlIdentifier.names.get(0)
    (tableName, Option.empty[String], mustJoin)
  }

  private def parseJoin(sqlNode: SqlNode): ArrayBuffer[JoinInfo] = {
    sqlNode match {
      case join: SqlJoin ⇒
        require(join.getCondition == null, "Join condition must be null: " + join.getCondition)
        require(join.getConditionType == JoinConditionType.NONE, "Join condition type must be None: " + join.getConditionType)
        require(join.getJoinType == JoinType.INNER || join.getJoinType == JoinType.LEFT, "join type must be inner join or left join: " + join.getJoinType)

        val mustJoin = join.getJoinType == JoinType.INNER
        val leftNode = join.getLeft
        val rightNode = join.getRight
        val leftJoins = parseJoin(leftNode)
        rightNode match {
          case call: SqlBasicCall ⇒ leftJoins.append(parseJoin(call, mustJoin))
          case identifier: SqlIdentifier ⇒ leftJoins.append(parseJoin(identifier, mustJoin))
          case _ ⇒ throw new RuntimeException("SqlNode must be basicCall or SQLIDENTIFIER_NAMES_FIELD: " + sqlNode)
        }
        leftJoins
      case basicCall: SqlBasicCall ⇒
        ArrayBuffer(parseJoin(basicCall, mustJoin = false))
      case _ ⇒ throw new RuntimeException("Parse Error! sqlNode is not sqlJoin: " + sqlNode)
    }
  }

  import java.util.{Set⇒JSet}
  def toJavaSet[T<:AnyRef](set:Set[T]):JSet[T]={
    val jSet = Sets.newHashSet[T]()
    set.foreach(e⇒jSet.add(e))
    jSet
  }

  private def parseCondition(names: java.util.List[String], streamTableName: String, tableRealNameMap: Map[String, String], compileContext: CompileContext): ConditionIdentifier = {
    if (names.size() == 1) {
      val field = names.get(0)
      val containsFieldTables = compileContext.queryExistsFieldSideTables(field, toJavaSet(tableRealNameMap.values.toSet))
      if (containsFieldTables.size() == 1) {
        val sideTable = containsFieldTables.iterator().next()
        return (sideTable, field, true)
      }
      if (compileContext.queryStreamSchema(streamTableName).containsKey(field)) {
        return (streamTableName, field, false)
      }
      throw new RuntimeException("Parse error! field reference " + field + " ambiguity or missing in join ( side tables: " + tableRealNameMap.values.mkString(",") + " )")
    } else {
      val table = tableRealNameMap(names.get(0))
      val field = names.get(1)
      if (compileContext.isSideTableRegister(table)) {
        return (table, field, true)
      } else if (compileContext.isStreamTableRegister(table)) {
        return (table, field, false)
      }
      throw new RuntimeException("Parse error! table reference " + table + " ambiguity or missing in join ( side tables: " + tableRealNameMap.values.mkString(",") + " )")
    }
  }

  private def parseCondition(condition: SqlBasicCall, conditions: ArrayBuffer[ConditionInfo], streamTable: String, tableRealNameMap: Map[String, String], context: CompileContext, mustJoinFlags: Map[String, Boolean]): Unit = {
    val sqlKind = condition.getKind
    if (sqlKind == SqlKind.AND) {
      parseCondition(condition.getOperands()(1).asInstanceOf[SqlBasicCall], conditions, streamTable, tableRealNameMap, context, mustJoinFlags)
      parseCondition(condition.getOperands()(0).asInstanceOf[SqlBasicCall], conditions, streamTable, tableRealNameMap, context, mustJoinFlags)
    } else if (condition.getOperator.isInstanceOf[SqlBinaryOperator]) {

      require(condition.getKind == SqlKind.EQUALS)
      val left = condition.getOperands()(0)
      val right = condition.getOperands()(1)

      require(left.isInstanceOf[SqlIdentifier] || right.isInstanceOf[SqlIdentifier], "Left or right must be SqlIdentifier")

      left match {
        case leftIdentifier: SqlIdentifier ⇒
          right match {
            case rightIdentifier: SqlIdentifier ⇒
              val (leftTableName, leftTableField, leftTableIsSide) = parseCondition(leftIdentifier.getNames, streamTable, tableRealNameMap, context)
              val (rightTableName, rightTableField, rightTableIsSide) = parseCondition(rightIdentifier.getNames, streamTable, tableRealNameMap, context)

              if (leftTableIsSide && !rightTableIsSide) {
                conditions.append(ConditionInfo(leftTableField,
                  leftTableName,
                  context.getFunctionCatalog.variable(rightTableName,
                    rightTableField,
                    context.queryStreamSchema(rightTableName).
                      get(rightTableField)),
                  mustJoinFlags(leftTableName)))
              } else if (!leftTableIsSide && rightTableIsSide) {
                conditions.append(ConditionInfo(rightTableField,
                  rightTableName,
                  context.getFunctionCatalog.variable(leftTableName,
                    leftTableField,
                    context.queryStreamSchema(leftTableName).
                      get(leftTableField))
                  , mustJoinFlags(rightTableName)))
              } else {
                throw new RuntimeException("Parse error! one field must be reference in stream table and other must be reference in side table")
              }
            case leftIdentifier: SqlIdentifier ⇒
              val names = leftIdentifier.getNames
              var sideField: String = null
              var sideTable: String = null
              if (names.size() == 1) {
                sideField = names.get(0)
                val containsFieldTables = context.queryExistsFieldSideTables(sideField, toJavaSet(tableRealNameMap.values.toSet))
                if (containsFieldTables.size() == 1) {
                  sideTable = containsFieldTables.iterator().next()
                  if (sideTable != streamTable) {
                    conditions.append(ConditionInfo(sideField, sideTable, SqlCompiler.genCode(right, sideTable, null, context), mustJoinFlags(sideTable)))
                  } else {
                    throw new RuntimeException("Parse error! field reference " + sideField + " in stream table")
                  }
                }
                throw new RuntimeException("Parse error! field reference " + sideField + " ambiguity or missing in join ( side tables: " + tableRealNameMap.values.mkString(",") + " )")
              } else {
                sideTable = tableRealNameMap(names.get(0))
                sideField = names.get(1)
                conditions.add(ConditionInfo(sideField, sideTable, SqlCompiler.genCode(left, streamTable, null, context), mustJoinFlags(sideTable)))
              }
          }
        case _ ⇒
          val names = right.asInstanceOf[SqlIdentifier].names
          var sideField: String = null
          var sideTable: String = null
          if (names.size() == 1) {
            sideField = names.get(0)
            val containsFieldTables = context.queryExistsFieldSideTables(sideField, toJavaSet(tableRealNameMap.values.toSet))
            if (containsFieldTables.size() == 1) {
              sideTable = containsFieldTables.iterator().next()
              if (sideTable != streamTable) {
                conditions.add(ConditionInfo(sideField, sideTable, SqlCompiler.genCode(left, sideTable, null, context), mustJoinFlags(sideTable)))
              } else throw {
                new RuntimeException("Parse error! field reference " + sideField + " in stream table")
              }
            }
            throw new RuntimeException("Parse error! field reference " + sideField + " ambiguity or missing in join ( side tables: " + tableRealNameMap.values.mkString(",") + " )")
          } else {
            sideTable = tableRealNameMap(names.get(0))
            sideField = names.get(1)
            conditions.append(ConditionInfo(sideField, sideTable, SqlCompiler.genCode(left, streamTable, null, context), mustJoinFlags(sideTable)))
          }
      }
    }
  }

  private def parseCondition(condition: SqlNode, streamTable: String, tableRealNameMap: Map[String, String], context: CompileContext, mustJoinFlags: Map[String, Boolean]): List[ConditionInfo] = {
    val conditions = ArrayBuffer[ConditionInfo]()
    parseCondition(condition.asInstanceOf[SqlBasicCall], conditions, streamTable, tableRealNameMap, context, mustJoinFlags)
    conditions.toList
  }

  private def constructTableRealNameMap(streamJoin: JoinInfo, sideJoins: ArrayBuffer[JoinInfo], compileContext: CompileContext): Map[String, String] = {

    val (streamTableName, streamTableAliasName, _) = streamJoin
    val tableRealNameMap = scala.collection.mutable.Map[String, String]()

    if (!compileContext.isStreamTableRegister(streamTableName)) {
      throw new RuntimeException("Parse Join Error! stream table: " + streamTableName + " does not register")
    }

    tableRealNameMap.put(streamTableAliasName.getOrElse(streamTableName), streamTableName)

    val alreadyJoinTables = Sets.newHashSet[String]()
    alreadyJoinTables.add(streamTableName)

    sideJoins.foreach { case (tableName, tableAliasName, _) ⇒

      if (!compileContext.isSideTableRegister(tableName)) {
        throw new RuntimeException("Parse Join Error! side table: " + tableName + " does not register")
      }

      if (alreadyJoinTables.contains(tableName)) {
        throw new RuntimeException("Parse Join Error! table: " + tableName + " already join ( already join alreadyJoinTables: " + String.join(",", alreadyJoinTables) + ")")
      }
      alreadyJoinTables.add(tableName)

      if (tableRealNameMap.contains(tableAliasName.getOrElse(tableName))) {
        throw new RuntimeException("Parse Join Error! table or alias : " + tableAliasName + " already join or define ( already join or define alreadyJoinTables: " + tableRealNameMap.values.mkString(",") + ")")
      }
      tableRealNameMap.put(tableAliasName.getOrElse(tableName), tableName)
    }
    tableRealNameMap.toMap
  }

  private def correctJoins(joins: ArrayBuffer[JoinInfo]): ArrayBuffer[JoinInfo] = {
    val correctJoins = ArrayBuffer[JoinInfo]()
    joins.zipWithIndex.foreach {
      case (joinInfo, index) ⇒
        if (index == 0 || index == joins.size - 1) {
          correctJoins.append(joinInfo)
        } else if (index < joins.size - 1) {
          correctJoins.append((joins(index)._1, joins(index)._2, joins(index + 1)._3))
        }
    }
    correctJoins
  }

  private def parseJoin(join: SqlJoin, context: CompileContext): JoinContext = {
    //维表信息
    val sideTables = correctJoins(parseJoin(join.getLeft))

    require(join.getConditionType == JoinConditionType.ON, " ConditionType must be ON")
    require(join.getJoinType == JoinType.INNER || join.getJoinType == JoinType.LEFT, "Join type must be inner join or left join: " + join.getJoinType)

    val mustJoin = join.getJoinType == JoinType.INNER
    join.getRight match {
      case basicCall: SqlBasicCall ⇒
        sideTables.append(parseJoin(basicCall, mustJoin))
      case identifier: SqlIdentifier ⇒
        sideTables.append(parseJoin(identifier, mustJoin))
      case _ ⇒
        throw new RuntimeException("SqlNode must be basicCall or SQLIDENTIFIER_NAMES_FIELD: " + join.getRight)
    }

    //流表信息
    val streamTableInfo = sideTables.remove(0)
    val condition = join.getCondition
    //校验最外层频道条件不能为空
    require(condition != null, "Join condition must be not null: " + join.getCondition)
    val tableRealNameMap = constructTableRealNameMap(streamTableInfo, sideTables, context)
    val conditions = parseCondition(condition, streamTableInfo._1, tableRealNameMap, context, sideTables.map(t ⇒ (t._1, t._3)).toMap)
    (streamTableInfo._1, conditions, tableRealNameMap, sideTables.map(_._1).toSet)
  }

  def apply(sqlNode: SqlNode, context: CompileContext): SideJoinRuntimeContext = {
    val sqlSelect = sqlNode.asInstanceOf[DslStreamSideJoinSqlSelect]

    //注册表
    val registerTable = sqlSelect.getAlias.getNames.get(0)
    //维流表连接信息
    val (sourceTable, conditions, tableRealNameMap, sideTables) = parseJoin(sqlSelect.getFrom.asInstanceOf[SqlJoin], context)
    //where计算表达式
    val whereEvaluation = Option.apply(SqlCompiler.genCode(sqlSelect.getWhere, sourceTable, JoinContext(sourceTable, sideTables, tableRealNameMap), context)).map(e ⇒ SqlCompiler.preDo(e).getValue)

    val selectEvaluations: ArrayBuffer[Evaluation] = ArrayBuffer()
    val selectNodes = sqlSelect.getSelectList.getList

    val registerNames = ArrayBuffer[String]()
    val registerTypes = ArrayBuffer[Class[_]]()

    selectNodes.zipWithIndex.foreach {
      case (node, index) ⇒
        val entry = SqlCompiler.rename(node, index)
        val selectInfo = SqlCompiler.preDo(SqlCompiler.genCode(entry.getKey, sourceTable, JoinContext(sourceTable, sideTables, tableRealNameMap), context))
        selectEvaluations.append(selectInfo.getValue)
        registerNames.append(entry.getValue)
        registerTypes.append(selectInfo.getKey)
    }

    new SideJoinRuntimeContext(sourceTable,
      selectEvaluations.toList,
      conditions,
      whereEvaluation,
      registerTable,
      context.queryStreamSchema(sourceTable).keySet().toArray(Array[String]()),
      registerNames.toArray,
      registerTypes.toArray)
  }
}
