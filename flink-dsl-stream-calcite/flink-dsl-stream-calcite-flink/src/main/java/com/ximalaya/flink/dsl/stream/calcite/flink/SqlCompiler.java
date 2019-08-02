package com.ximalaya.flink.dsl.stream.calcite.flink;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.side.SideTableInfo;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamExplodeSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSideJoinSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSimpleExplodeSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.*;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import com.ximalaya.flink.dsl.stream.calcite.parser.DslStreamParserUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/31
 **/

public class SqlCompiler {


    public static final String FIELD_SEPARATOR = "$";

    private static Field SQL_IDENTIFIER_NAMES_FIELD;

    static {
        try {
            SQL_IDENTIFIER_NAMES_FIELD = SqlIdentifier.class.getDeclaredField("names");
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<String> getNames(SqlIdentifier sqlIdentifier) {
        try {
            return (List<String>) SQL_IDENTIFIER_NAMES_FIELD.get(sqlIdentifier);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 对可执行表达式进行预处理
     * @param evaluation 可执行表达式对象
     * @return 预处理后的可执行表达式对象
     */
    public static Map.Entry<Class<?>,Evaluation> preDo(Evaluation evaluation){
        evaluation.checkAndGetReturnType();
        return Pair.of(evaluation.checkAndGetReturnType(),evaluation.eager());
    }


    /**
     * 对查询的列进行重命名
     * @param sqlNode 查询的列节点
     * @param index 查询的列节点索引值
     * @return 返回查询的列节点和对应重命名名字
     */
    public static Map.Entry<SqlNode, String> rename(SqlNode sqlNode,int index) {
        if (sqlNode.getKind() == SqlKind.AS) {
            List<SqlNode> sqlNodes = ((SqlBasicCall) sqlNode).getOperandList();
            Preconditions.checkArgument(sqlNodes.size() == 2);
            return Pair.of(sqlNodes.get(0), getNames((SqlIdentifier) (sqlNodes.get(1))).get(0));
        } else if (sqlNode.getKind() == SqlKind.IDENTIFIER) {
            List<String> names = getNames((SqlIdentifier) sqlNode);
            if(names.size()==1) {
                return Pair.of(sqlNode, names.get(0));
            }else{
                return Pair.of(sqlNode, names.get(1));
            }
        } else {
            return Pair.of(sqlNode, FIELD_SEPARATOR + index);
        }
    }

    /**
     * 基于查询的列生成可执行表达式
     * @param sqlNode 查询的列节点
     * @param tableName 列节点源表信息
     * @param joinContext 表连接上下文
     * @param context 查询上下文
     * @return 可执行表达式
     */
    public static Evaluation genCode(SqlNode sqlNode,
                                     String tableName,
                                     JoinContext joinContext,
                                     CompileContext context) {
        if (sqlNode == null) {
            return null;
        }
        //todo
        if (sqlNode instanceof SqlLiteral) {
            Object value = ((SqlLiteral) sqlNode).getValue();
            if (value instanceof BigDecimal) {
                BigDecimal bigDecimal = (BigDecimal)value;
                if(bigDecimal.scale() == 0) {
                    return context.getFunctionCatalog().
                            constant(bigDecimal.longValue());
                }else{
                    return context.getFunctionCatalog().
                            constant(bigDecimal.doubleValue());
                }
            }
            if (value instanceof NlsString) {
                return context.getFunctionCatalog().
                        constant(((NlsString) value).getValue());
            }
            return context.getFunctionCatalog().
                    constant(((SqlLiteral) sqlNode).
                            getValue());
        }
        if (sqlNode instanceof SqlIdentifier) {
            if(joinContext==null) {
                String variable = getNames((SqlIdentifier) sqlNode).get(0);
                return context.getFunctionCatalog().
                        variable(variable,
                                context.queryStreamSchema(tableName).get(variable));
            }else{
                List<String> names =  getNames((SqlIdentifier)sqlNode);
                if(names.size() == 1){
                    String variable = names.get(0);
                    Map<String,Class<?>> schema = context.queryStreamSchema(joinContext.streamTable());
                    Set<String> sideTables = context.queryExistsFieldSideTables(variable,joinContext.sideTables());

                    if(schema.containsKey(variable) && sideTables.isEmpty()){
                        return context.getFunctionCatalog().
                                variable(joinContext.streamTable(),variable,
                                        schema.get(variable));
                    }else if(!schema.containsKey(variable) && sideTables.size() == 1){
                        String sideTable = sideTables.iterator().next();
                        Optional<SideTableInfo> sideSchemaContext = context.querySideSchema(sideTable);

                        if(sideSchemaContext.isPresent()) {
                            return context.getFunctionCatalog().variable(sideTable,
                                    variable,
                                    sideSchemaContext.get().logicSchema().get(variable));
                        }else{
                            throw new RuntimeException("Parse error! table: "+sideTable+" schema not found");
                        }
                    }else{
                        throw new RuntimeException("Parse error! field reference "+ variable+" ambiguity or missing in join (stream table: "+joinContext.streamTable()+" " +
                                "side tables: "+String.join(",",sideTables)+" )");
                    }
                }else{
                    String tableOrAlias = names.get(0);
                    String variable = names.get(1);
                    Map<String,String> tableRealNameMap = joinContext.tableRealNameMap();

                    if(tableRealNameMap.containsKey(tableOrAlias)) {
                        String realName = tableRealNameMap.get(tableOrAlias);
                        Map<String, Class<?>> schema = context.queryStreamSchema(joinContext.streamTable());
                        Optional<SideTableInfo> sideSchemaContext = context.querySideSchema(realName);

                        if (sideSchemaContext.isPresent()) {
                            return context.getFunctionCatalog().variable(realName, variable, sideSchemaContext.get().logicSchema().get(variable));
                        } else if(!schema.isEmpty()){
                            return context.getFunctionCatalog().variable(realName, variable, schema.get(variable));
                        } else {
                            throw new RuntimeException("Parse error! table or alias: "+tableOrAlias+" is not found or define (stream table: "+joinContext.streamTable()+" "+
                                    "side tables: "+String.join(",",joinContext.sideTables())+")");
                        }
                    }
                }
            }
        }
        if (sqlNode instanceof SqlCase) {
            SqlCase sqlCase = (SqlCase) sqlNode;
            Map<Evaluation, Evaluation> conditions = Maps.newHashMap();
            List<Evaluation> whenEvaluation = sqlCase.getWhenOperands().getList().
                    stream().map(node -> genCode(node, tableName, joinContext, context)).collect(Collectors.toList());
            List<Evaluation> thenEvaluation = sqlCase.getThenOperands().getList().
                    stream().map(node -> genCode(node, tableName, joinContext,context)).collect(Collectors.toList());
            Preconditions.checkArgument(whenEvaluation.size() == thenEvaluation.size());
            for (int i = 0; i < whenEvaluation.size(); i++) {
                conditions.put(whenEvaluation.get(i), thenEvaluation.get(i));
            }
            Evaluation guard = sqlCase.getElseOperand() == null ? null : genCode(sqlCase.getElseOperand(), tableName, joinContext, context);
            return context.getFunctionCatalog().caseWhen(conditions, guard);
        }
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlKind sqlKind = sqlBasicCall.getKind();
            if (sqlKind == SqlKind.SIMILAR) {
                boolean similar = !sqlBasicCall.getOperator().getName().startsWith("NOT");
                return context.getFunctionCatalog().
                        similar(genCode(sqlBasicCall.operands[0], tableName, joinContext,context),
                                genCode(sqlBasicCall.operands[1], tableName, joinContext,context),
                                sqlBasicCall.operands.length == 2 ? null :
                                        genCode(sqlBasicCall.operands[2],
                                                tableName, joinContext, context), similar);
            }
            if (sqlKind == SqlKind.BETWEEN) {
                SqlBetweenOperator operator = ((SqlBetweenOperator)sqlBasicCall.getOperator());
                boolean includeBound = "ASYMMETRIC".equals(operator.flag.name());
                boolean between = "NOT BETWEEN".equals(operator.getName());
                List<SqlNode> operands = sqlBasicCall.getOperandList();
                return context.getFunctionCatalog().between(genCode(operands.get(0), tableName, joinContext, context),
                        genCode(operands.get(1), tableName, joinContext,context),
                        genCode(operands.get(2), tableName, joinContext,context), includeBound, between);
            }
            if (sqlKind == SqlKind.CAST) {
                SqlNode[] operands = sqlBasicCall.getOperands();
                Evaluation single = genCode(operands[0], tableName, joinContext,context);
                SqlDataTypeSpec sqlDataTypeSpec = ((SqlDataTypeSpec)operands[1]);
                return context.getFunctionCatalog().cast(single, sqlDataTypeSpec);
            }
            if(sqlKind == SqlKind.MAP_VALUE_CONSTRUCTOR){
                SqlNode[] operands = sqlBasicCall.getOperands();
                Map<Evaluation,Evaluation> map = Maps.newHashMap();
                for(SqlNode operand:operands){
                    if(operand instanceof SqlBasicCall && operand.getKind() == SqlKind.ROW){
                        SqlNode[] nodes = ((SqlBasicCall) operand).getOperands();
                        Preconditions.checkArgument(nodes.length == 2);
                        map.put(genCode(nodes[0],tableName, joinContext,context),
                                genCode(nodes[1],tableName, joinContext,context));
                    }
                }
                return context.getFunctionCatalog().mapConstruct(map);
            }
            if(sqlKind == SqlKind.IN){
                SqlNode[] sqlNodes = sqlBasicCall.getOperands();
                Preconditions.checkArgument(sqlNodes.length == 2);
                Evaluation left = genCode(sqlNodes[0],tableName, joinContext,context);
                List<Evaluation> rightList = ((SqlNodeList)sqlNodes[1]).getList().stream().map(node->genCode(node,tableName, joinContext,context)).collect(Collectors.toList());
                return context.getFunctionCatalog().in(left,rightList);
            }
            if(sqlKind == SqlKind.NOT_IN){
                SqlNode[] sqlNodes = sqlBasicCall.getOperands();
                Preconditions.checkArgument(sqlNodes.length == 2);
                Evaluation left = genCode(sqlNodes[0],tableName, joinContext,context);
                List<Evaluation> rightList = ((SqlNodeList)sqlNodes[1]).getList().stream().map(node->genCode(node,tableName, joinContext,context)).collect(Collectors.toList());
                return context.getFunctionCatalog().notIn(left,rightList);

            }
            List<SqlNode> operators = sqlBasicCall.getOperandList();
            try {
                return context.getFunctionCatalog().createFunction(sqlBasicCall.getOperator().getName()
                        , sqlKind,
                        operators.stream().map(node -> genCode(node, tableName, joinContext,context)).toArray());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("can not parse this node: "+sqlNode);
    }

    public static SqlRuntimeContext compile(String sql, CompileContext compileContext) throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse(sql);
        if (sqlNode instanceof DslStreamSimpleExplodeSqlSelect) {
            return QueryRuntimeContext.createSimpleExplodeRuntimeContext(sqlNode,compileContext);
        }
        if (sqlNode instanceof DslStreamExplodeSqlSelect) {
            return QueryRuntimeContext.createExplodeRuntimeContext(sqlNode,compileContext);
        }
        if (sqlNode instanceof DslStreamSideJoinSqlSelect){
            return SideJoinRuntimeContext.apply(sqlNode,compileContext);
        }
        return null;
    }
}
