package com.ximalaya.flink.dsl.stream.calcite.flink.context;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ximalaya.flink.dsl.stream.side.SideTableInfo;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.scala.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/31
 **/

public class CompileContext {

    private StreamTableEnvironment tableEnvironment;

    private Map<String,Map<String,Class<?>>> streamSchemaCache = Maps.newHashMap();

    private Map<String, SideTableInfo> sideTableInfos = Maps.newHashMap();

    private FunctionCatalog functionCatalog;


    private SideCatalog sideCatalog;


    /**
     * 检测某个流表是否被注册
     * @param tableName 流表名
     * @return 是否被注册
     */
    public boolean isStreamTableRegister(String tableName){
        return tableEnvironment.getTable(tableName).isDefined();
    }

    /**
     * 检查某个维表是否被注册
     * @param tableName 维表名
     * @return 是否被注册
     */
    public boolean isSideTableRegister(String tableName){
        return sideTableInfos.containsKey(tableName);
    }

    /**
     * 查询某个流表的schema信息
     * @param tableName 流表名
     * @return schema信息
     */
    public Map<String,Class<?>> queryStreamSchema(String tableName){
        if(streamSchemaCache.containsKey(tableName)){
            return streamSchemaCache.get(tableName);
        }

        if(isStreamTableRegister(tableName)) {
            Table table = tableEnvironment.sqlQuery("select * from " + tableName);
            Map<String, Class<?>> schema = Maps.newLinkedHashMap();
            for (int i = 0; i < table.getSchema().getColumnCount(); i++) {
                schema.put(table.getSchema().getColumnNames()[i],
                        table.getSchema().getTypes()[i].getTypeClass());
            }
            streamSchemaCache.put(tableName, schema);
            return schema;
        }else{
            return Maps.newHashMap();
        }
    }

    /**
     * 查询某个维表的schema信息
     * @param tableName 维表名
     * @return schema信息
     */
    public Optional<SideTableInfo> querySideSchema(String tableName){
        return Optional.ofNullable(sideTableInfos.get(tableName));
    }

    /**
     * 查询包含该字段的维表集合
     * @param fieldName 被查询的字段名
     * @param sideTables 被查询的维表集合
     * @return 包含该字段的维表集合
     */
    public Set<String> queryExistsFieldSideTables(String fieldName,Set<String> sideTables){
        Set<String> containTables = Sets.newHashSet();
        sideTableInfos.forEach((table, sideTableInfo)->{
            if(sideTableInfo.logicSchema().containsKey(fieldName)){
                containTables.add(table);
            }
        });
        return containTables;
    }


    public SideCatalog getSideCatalog(){ return sideCatalog;}

    public FunctionCatalog getFunctionCatalog(){
        return functionCatalog;
    }

    public StreamTableEnvironment getTableEnvironment(){ return tableEnvironment;}

    public CompileContext(StreamTableEnvironment tableEnvironment, SideCatalog sideCatalog,
                          Map<String, ScalarFunction> dynamicFunctions,Map<String, SideTableInfo> sideTableInfos) throws Exception {
        this.tableEnvironment = tableEnvironment;
        this.sideCatalog = sideCatalog;
        this.sideTableInfos = sideTableInfos;
        this.functionCatalog = new FunctionCatalog.Builder(new Class<?>[]{
                ArithmeticFunctions.class,
                BuildInUserDefineFunction.class,
                CollectionFunctions.class,
                ComparisonFunctions.class,
                ConditionalFunctions.class,
                HashFunctions.class,
                LogicalFunctions.class,
                StringFunctions.class,
                TemporalFunctions.class,
                TypeConversionFunctions.class,
                ValueConstructionFunctions.class
        }).setDynamicFunctions(dynamicFunctions).build();
    }
}
