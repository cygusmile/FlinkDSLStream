package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.getCommonSupperClass;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/

public class ValueConstructionFunctions {

    @EvaluationInfo(name = "ARRAY",
            desc = "returns an array created from a list of values (value1, value2, ...).",
            kind = SqlKind.ARRAY_VALUE_CONSTRUCTOR)
    public static class ArrayConstruct extends BaseListEvaluation {

        private Class<?> returnType;

        public ArrayConstruct(List<Evaluation> list) {
            super(list);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Stream<Object> a = list.stream().map(e->e.eval(row));
            if ( returnType == int[].class){
                return a.mapToInt(e->(int)e).toArray();
            }
            if( returnType == float[].class){
                Object[] b = a.toArray();
                float[] c = new float[b.length];
                for(int i=0;i<b.length;i++){
                    c[i] = (float)b[i];
                }
                return c;
            }
            if( returnType == long[].class ){
                return a.mapToLong(e->(long)e).toArray();
            }
            if( returnType == double[].class ){
                return a.mapToDouble(e->(double)e).toArray();
            }
            if( returnType == boolean[].class){
                Object[] b = a.toArray();
                boolean[] c = new boolean[b.length];
                for(int i=0;i<b.length;i++){
                    c[i] = (boolean)b[i];
                }
                return c;
            }
            if( returnType == String[].class){
                Object[] b = a.toArray();
                String[] c = new String[b.length];
                for(int i=0;i<b.length;i++){
                    c[i] = (String)b[i];
                }
                return c;
            }
            return a.toArray();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            returnType =  Object[].class;
            Class<?>[] classes = new Class<?>[list.size()];
            for(int i = 0; i< list.size(); i++){
                classes[i] = list.get(i).checkAndGetReturnType();
            }
            Class<?> a =  getCommonSupperClass(classes);
            if( a == Integer.TYPE ){
                returnType =  int[].class;
            }
            if( a == Long.TYPE ){
                returnType = long[].class;
            }
            if( a == Float.TYPE ){
                returnType =  float[].class;
            }
            if( a == Double.TYPE ){
                returnType =  double[].class;
            }
            if( a == Boolean.TYPE){
                returnType =  boolean[].class;
            }
            if( a == String.class){
                returnType =  String[].class;
            }
            return returnType;
        }
    }

    @EvaluationInfo(name = "MAP",
            desc = "returns a map created from a list of key-value pairs ((value1, value2), (value3, value4), ...).",
            kind = SqlKind.MAP_VALUE_CONSTRUCTOR)
    public static class MapConstruct extends BaseMapEvaluation{
        public MapConstruct(Map<Evaluation, Evaluation> map) {
            super(map);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Map<Object,Object> map = Maps.newLinkedHashMap();
            for(Map.Entry<Evaluation,Evaluation> entry:this.map.entrySet()){
                map.put(entry.getKey().eval(row),entry.getValue().eval(row));
            }
            return map;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            map.forEach((k,v)->{
                k.checkAndGetReturnType();
                v.checkAndGetReturnType();
            });
            return Map.class;
        }
    }

    @EvaluationInfo(name = "ROW",
            desc = "returns a row created from a list of values (value1, value2,...).",
            kind = SqlKind.ROW)
    public static class RowConstruct extends BaseListEvaluation{
        public RowConstruct(List<Evaluation> list) {
            super(list);
        }
        @Override
        public Object eval(Map<String, Object> row) {
           return Row.of(list.stream().map(e->e.eval(row)).toArray());
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            list.forEach(Evaluation::checkAndGetReturnType);
            return Row.class;
        }
    }
}
