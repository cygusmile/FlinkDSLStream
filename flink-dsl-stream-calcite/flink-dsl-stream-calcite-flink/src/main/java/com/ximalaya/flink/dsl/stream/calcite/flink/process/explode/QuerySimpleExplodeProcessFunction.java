package com.ximalaya.flink.dsl.stream.calcite.flink.process.explode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.QueryRuntimeContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/8
 **/

public class QuerySimpleExplodeProcessFunction extends ProcessFunction<Row,Row> {

    protected final Evaluation explodeFieldEvaluation;
    protected final String[] fields;

    protected final Map<String,Object> convert(Row row,String[] fields){
        Map<String,Object> value = Maps.newHashMap();
        for(int i=0;i<fields.length;i++){
            value.put(fields[i],row.getField(i));
        }
        return value;
    }

    protected final void collect(Object a,int explodeFieldIndex, List<Object> list,Collector<Row> out){
        if(a.getClass().getComponentType() == Integer.TYPE){
            for(int b:(int[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        } else if(a.getClass().getComponentType() == Double.TYPE){
            for(double b:(double[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        } else if(a.getClass().getComponentType() == Long.TYPE){
            for(long b:(long[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        } else  if(a.getClass().getComponentType() == Float.TYPE){
            for(float b:(float[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        } else  if(a.getClass().getComponentType() == Boolean.TYPE){
            for(boolean b:(boolean[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        } else if(a.getClass().getComponentType() == Byte.TYPE){
            for(byte b:(byte[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        } else{
            for(Object b:(Object[])a){
                if(list==null) {
                    out.collect(Row.of(b));
                }else{
                    List<Object> result = Lists.newArrayList(list);
                    result.add(explodeFieldIndex,b);
                    out.collect(Row.of(result.toArray()));
                }
            }
        }
    }

    public QuerySimpleExplodeProcessFunction(Evaluation explodeFieldEvaluation,
                                             String[] fields){
        this.explodeFieldEvaluation = explodeFieldEvaluation;
        this.fields = fields;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        explodeFieldEvaluation.open(getRuntimeContext());
    }

    @Override
    public void close() throws Exception {
        explodeFieldEvaluation.close();
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
        Object a = explodeFieldEvaluation.eval(convert(value,fields));
        collect(a,-1,null,out);
    }
}
