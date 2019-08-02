package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/

public class Variable implements Evaluation{
    protected String name;
    private Class<?> type;
    public Variable(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }
    @Override
    public Object eval(Map<String, Object> row) {
        return row.get(name);
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        if(row.containsKey(name)){
            return new Constant(row.get(name));
        }else{
            return this;
        }
    }

    @Override
    public Class<?> checkAndGetReturnType() {
        return type;
    }

    public String getName() {
        return name;
    }
}

