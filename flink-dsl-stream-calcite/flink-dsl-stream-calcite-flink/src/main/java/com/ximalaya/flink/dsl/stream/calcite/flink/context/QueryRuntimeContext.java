package com.ximalaya.flink.dsl.stream.calcite.flink.context;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamExplodeSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSimpleExplodeSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.flink.SqlCompiler;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/8
 **/

public class QueryRuntimeContext implements SqlRuntimeContext {

    /**
     * 来源表名字 表示从哪个表读取数据
     */
    private final String sourceTable;
    /**
     * 注册表名字 表示用该表名字将表注册到作业中
     */
    private final String registerTable;
    /**
     * 普通字段表达式列表
     */
    private final List<Evaluation> selectEvaluations;
    /**
     * 过滤条件执行代码
     */
    private final Optional<Evaluation> filterEvaluation;
    /**
     * 扁平字段表达式和其别名
     */
    private final Optional<Map.Entry<Integer, Evaluation>> explodeFieldEvaluation;
    /**
     * 源表字段列表
     */
    private final String[] sourceNames;
    /**
     * 注册表字段列表
     */
    private final String[] registerNames;
    /**
     * 注册表字段类型列表
     */
    private final Class<?>[] registerTypes;

    private QueryRuntimeContext(String sourceTable,
                                String registerTable,
                                List<Evaluation> selectEvaluations,
                                Optional<Evaluation> filterEvaluation,
                                Optional<Map.Entry<Integer, Evaluation>> explodeFieldEvaluation,
                                String[] sourceNames,
                                String[] registerNames,Class<?> []registerTypes) {
        this.sourceTable = sourceTable;
        this.registerTable = registerTable;
        this.selectEvaluations = selectEvaluations;
        this.filterEvaluation = filterEvaluation;
        this.explodeFieldEvaluation = explodeFieldEvaluation;
        this.sourceNames = sourceNames;
        this.registerNames = registerNames;
        this.registerTypes = registerTypes;
    }

    public static class Builder {
        private final String sourceTable;
        private final String registerTable;
        private final String[] sourceNames;
        private final String[] registerNames;
        private final Class<?>[] regiserTypes;

        private List<Evaluation> selectEvaluations = Lists.newArrayList();
        private Optional<Evaluation> filterEvaluation = Optional.empty();
        private Optional<Map.Entry<Integer, Evaluation>> explodeFieldEvaluation = Optional.empty();

        public Builder(String sourceTable, String registerTable,String[] sourceNames, String[] registerNames,Class<?>[] regiserTypes) {
            this.sourceTable = sourceTable;
            this.registerTable = registerTable;
            this.sourceNames = sourceNames;
            this.registerNames = registerNames;
            this.regiserTypes = regiserTypes;
        }

        public Builder setFilterEvaluation(Evaluation evaluation) {
            Preconditions.checkNotNull(evaluation);
            this.filterEvaluation = Optional.of(evaluation);
            return this;
        }

        public Builder setExplodeFieldEvaluation(int index, Evaluation evaluation) {
            Preconditions.checkNotNull(evaluation);
            this.explodeFieldEvaluation = Optional.of(Pair.of(index, evaluation));
            return this;
        }

        public Builder setSelectEvaluations(List<Evaluation> evaluations) {
            Preconditions.checkArgument(!evaluations.isEmpty());
            this.selectEvaluations = evaluations;
            return this;
        }

        public QueryRuntimeContext build() {
            return new QueryRuntimeContext(sourceTable, registerTable, selectEvaluations,
                    filterEvaluation, explodeFieldEvaluation,sourceNames, registerNames,regiserTypes);
        }
    }

    public String getSourceTable() {
        return sourceTable;
    }
    public String getRegisterTable() {
        return registerTable;
    }
    public Optional<Map.Entry<Integer, Evaluation>> getExplodeFieldEvaluation() {
        return explodeFieldEvaluation;
    }
    public String[] getRegisterNames() {
        return registerNames;
    }
    public Class<?>[] getRegisterTypes(){
        return registerTypes;
    }
    public String[] getSourceNames() { return sourceNames;}
    public Optional<Evaluation> getFilterEvaluation() {
        return filterEvaluation;
    }
    public List<Evaluation> getSelectEvaluations() {
        return selectEvaluations;
    }
    public static QueryRuntimeContext createSimpleExplodeRuntimeContext(SqlNode sqlNode,
                                                                        CompileContext compileContext) {
        DslStreamSimpleExplodeSqlSelect sqlSelect = (DslStreamSimpleExplodeSqlSelect) sqlNode;
        //源表
        String sourceTable = sqlSelect.getFrom().names.get(0);
        //注册表
        String registerTable = sqlSelect.getAlias().names.get(0);
        //扁平字段执行表达式
        Evaluation explodeFieldEvaluation = SqlCompiler.genCode(sqlSelect.getExplodeField(), sourceTable,null,compileContext);
        //扁平字段别名
        String explodeFieldAlias = Optional.
                ofNullable(sqlSelect.getFieldAlias()).
                map(f -> f.names.get(0)).orElse("_0");

        Map.Entry<Class<?>,Evaluation> explodeFieldInfo = SqlCompiler.preDo(explodeFieldEvaluation);
        QueryRuntimeContext.Builder builder = new QueryRuntimeContext.Builder(sourceTable,
                registerTable,
                compileContext.queryStreamSchema(sourceTable).keySet().toArray(new String[]{}),
                new String[]{explodeFieldAlias},new Class<?>[]{explodeFieldInfo.getKey().getComponentType()});
        builder.setExplodeFieldEvaluation(0, explodeFieldInfo.getValue());

        //过滤条件执行表达式
        Optional.ofNullable(SqlCompiler.
                genCode(sqlSelect.getWhere(), sourceTable, null,compileContext)).ifPresent(evaluation -> {
            builder.setFilterEvaluation(SqlCompiler.preDo(evaluation).getValue());
        });
        return builder.build();
    }

    public static QueryRuntimeContext createExplodeRuntimeContext(SqlNode sqlNode, CompileContext compileContext) {
        DslStreamExplodeSqlSelect sqlSelect = (DslStreamExplodeSqlSelect) sqlNode;
        //源表
        String sourceTable = sqlSelect.getFrom().names.get(0);
        //注册表
        String registerTable = sqlSelect.getAlias().names.get(0);
        //扁平字段执行表达式
        Evaluation explodeFieldEvaluation = SqlCompiler.genCode(sqlSelect.getExplodeField(), sourceTable,null, compileContext);
        //扁平字段别名
        String explodeFieldAlias = sqlSelect.getExplodeAlias().names.get(0);

        List<SqlNode> selectNodes = sqlSelect.getSelectList().getList();
        List<Evaluation> selectEvaluations = Lists.newArrayList();
        String[] registerNames = new String[selectNodes.size()];
        Class<?>[] registerTypes = new Class<?>[selectNodes.size()];

        Map.Entry<Class<?>,Evaluation> explodeFieldInfo = SqlCompiler.preDo(explodeFieldEvaluation);

        int explodeFieldIndex = -1;
        for (int i = 0; i < selectNodes.size(); i++) {
            Map.Entry<SqlNode, String> entry = SqlCompiler.rename(selectNodes.get(i), i);
            if (entry.getKey() instanceof SqlIdentifier &&
                    ((SqlIdentifier) entry.getKey()).names.get(0).equals(explodeFieldAlias)) {
                explodeFieldIndex = i;
                registerTypes[i] = explodeFieldInfo.getKey().getComponentType();
            }else {
                Map.Entry<Class<?>, Evaluation> selectInfo = SqlCompiler.preDo(SqlCompiler.genCode(entry.getKey(),
                        sourceTable, null,compileContext));
                selectEvaluations.add(selectInfo.getValue());
                registerTypes[i] = selectInfo.getKey();
            }
            registerNames[i] = entry.getValue();
        }
        QueryRuntimeContext.Builder builder = new QueryRuntimeContext.Builder(sourceTable,
                registerTable,
                compileContext.queryStreamSchema(sourceTable).keySet().toArray(new String[]{}),
                registerNames,registerTypes);
        builder.setExplodeFieldEvaluation(explodeFieldIndex,explodeFieldInfo.getValue());
        builder.setSelectEvaluations(selectEvaluations);
        //过滤条件执行表达式
        Optional.ofNullable(SqlCompiler.
                genCode(sqlSelect.getWhere(), sourceTable,null, compileContext)).ifPresent(evaluation -> {
            builder.setFilterEvaluation(SqlCompiler.preDo(evaluation).getValue());
        });
        return builder.build();
    }
}
