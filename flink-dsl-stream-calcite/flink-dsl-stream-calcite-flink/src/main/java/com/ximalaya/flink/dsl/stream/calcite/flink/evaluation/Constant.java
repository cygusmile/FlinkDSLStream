package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/

public class Constant implements Evaluation{
    private final Object constant;
    public Constant(Object constant) {
        this.constant = constant;
    }
    @Override
    public Object eval(Map<String, Object> row) {
        return constant;
    }
    @Override
    public Class<?> checkAndGetReturnType() {
        Class<?> a = constant.getClass();
        if( a == Integer.class){
            return Integer.TYPE;
        }
        if( a == Double.class){
            return Double.TYPE;
        }
        if( a == Float.class){
            return Float.TYPE;
        }
        if( a == Long.class ){
            return Long.TYPE;
        }
        if( a == Boolean.class){
            return Boolean.TYPE;
        }
        return a;
    }
}
