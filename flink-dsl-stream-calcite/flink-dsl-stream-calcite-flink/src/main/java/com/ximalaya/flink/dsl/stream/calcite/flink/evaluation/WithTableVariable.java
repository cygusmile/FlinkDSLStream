package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.ximalaya.flink.dsl.stream.calcite.flink.SqlCompiler;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/16
 **/

public class WithTableVariable extends Variable {

    private String tableName;

    public WithTableVariable(String name,String tableName,Class<?> type) {
        super(name,type);
        this.tableName = tableName;
    }


    @Override
    public Object eval(Map<String, Object> row) {
        return row.get(tableName+ SqlCompiler.FIELD_SEPARATOR +name);
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        if(row.containsKey(tableName+SqlCompiler.FIELD_SEPARATOR+name)){
            return new Constant(row.get(tableName+SqlCompiler.FIELD_SEPARATOR+name));
        }else{
            return this;
        }
    }
}
