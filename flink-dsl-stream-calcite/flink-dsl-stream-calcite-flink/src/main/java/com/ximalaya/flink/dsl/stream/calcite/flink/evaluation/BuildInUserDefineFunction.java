package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.*;
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/3
 **/
public class BuildInUserDefineFunction {
    //todo does not complete
    @EvaluationInfo(name = "ArraySize",
            desc = "compute array size",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ArraySize extends BaseUnaryEvaluation {
        public ArraySize(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if( a.getClass().getComponentType() == Integer.TYPE) {
                return ((int[]) a).length;
            }
            if( a.getClass().getComponentType() == Float.TYPE){
                return ((float[])a).length;
            }
            if( a.getClass().getComponentType() == Double.TYPE){
                return ((double[])a).length;
            }
            if( a.getClass().getComponentType() == Long.TYPE){
                return ((long[])a).length;
            }
            return ((Object[])a).length;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray());
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "ArraySum",
            desc = "calculate the sum of an array",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ArraySum extends BaseUnaryEvaluation {
        public ArraySum(Evaluation single)  {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (isIntArray(a)) {
                return Arrays.stream(((int[]) a)).sum();
            }
            if (isLongArray(a)) {
                return Arrays.stream(((long[])a)).sum();
            }
            if (isFloatArray(a)) {
                float[] b = (float[])a;
                float sum = 0;
                for(float b1:b) {
                    sum += b1;

                }
                return sum;
            }if (isDoubleArray(a)) {
                return Arrays.stream(((double[]) a)).sum();
            }
            if (isIntArrayArray(a)) {
                return Arrays.stream(((int[][]) a)).
                        map(e-> Arrays.stream(e).sum()).
                        mapToInt(e->e).sum();
            }
            if (isLongArrayArray(a)) {
                return Arrays.stream(((long[][]) a)).
                        map(e-> Arrays.stream(e).sum()).
                        mapToLong(e->e).sum();
            }
            if (isDoubleArrayArray(a)) {
                return Arrays.stream(((double[][]) a)).
                        map(e-> Arrays.stream(e).sum()).
                        mapToDouble(e->e).sum();
            }
            float[][] b = (float[][])a;
            float sum = 0;
            for(float[] b1:b){
                for(float b2:b1){
                    sum+=b2;
                }
            }
            return sum;
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray());
            Preconditions.checkArgument(a.getComponentType() == Integer.TYPE ||
                    a.getComponentType() == Double.TYPE ||
                    a.getComponentType() == Long.TYPE ||
                    a.getComponentType() ==Float.TYPE);
            return a.getComponentType();
        }
    }

    @EvaluationInfo(name = "ArrayIndex",
            desc = "returns the array element at the right position",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ArrayIndex extends BaseBinaryEvaluation{
        public ArrayIndex(Evaluation left, Evaluation right) {
           super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            if( a.getClass().getComponentType() == Integer.TYPE) {
                return ((int[]) a)[(int) right.eval(row)];
            }
            if( a.getClass().getComponentType() == Float.TYPE){
                return ((float[])a)[(int) right.eval(row)];
            }
            if( a.getClass().getComponentType() == Double.TYPE){
                return ((double[])a)[(int) right.eval(row)];
            }
            if( a.getClass().getComponentType() == Long.TYPE){
                return ((long[])a)[(int) right.eval(row)];
            }
            return ((Object[])a)[(int) right.eval(row)];
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray());
            Preconditions.checkArgument(b == Integer.TYPE);
            return a.getComponentType();
        }
    }

    @EvaluationInfo(name = "ArrayJoin",
            desc = "connect an array",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ArrayJoin extends BaseBinaryEvaluation{
        public ArrayJoin(Evaluation left, Evaluation right) {
            super(left,right);
        }

        private String join(Stream<String> stream,Map<String,Object> row){
            return stream.collect(Collectors.joining((String) right.eval(row)));
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            if( a.getClass().getComponentType() == Integer.TYPE) {
                return join(Arrays.stream((int[]) left.eval(row)).mapToObj(String::valueOf), row);
            }
            if( a.getClass().getComponentType() == Long.TYPE){
                return join(Arrays.stream((long[]) left.eval(row)).mapToObj(String::valueOf), row);
            }
            if( a.getClass().getComponentType() == Double.TYPE){
                return join(Arrays.stream((double[]) left.eval(row)).mapToObj(String::valueOf), row);
            }
            if( a.getClass().getComponentType() == Float.TYPE){
                float[] b = (float[])a;
                List<String> c = Lists.newArrayList();
                for(float d:b){
                    c.add(String.valueOf(d));
                }
                return String.join((String)right.eval(row),c);
            }
            if( a.getClass().getComponentType() == Boolean.TYPE){
                boolean[] b = (boolean[])a;
                List<String> c = Lists.newArrayList();
                for(boolean d:b){
                    c.add(String.valueOf(d));
                }
                return String.join((String)right.eval(row),c);
            }
            return join(Arrays.stream((Object[])left.eval(row)).map(Object::toString),row);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray());
            Preconditions.checkArgument(b == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "ArrayHead",
            desc = "get an array header element",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ArrayHead extends BaseUnaryEvaluation{
        public ArrayHead(Evaluation single){
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if( a.getClass().getComponentType() == Integer.TYPE) {
                return ((int[]) a)[0];
            }
            if( a.getClass().getComponentType() == Float.TYPE){
                return ((float[])a)[0];
            }
            if( a.getClass().getComponentType() == Double.TYPE){
                return ((double[])a)[0];
            }
            if( a.getClass().getComponentType() == Long.TYPE){
                return ((long[])a)[0];
            }
            return ((Object[])a)[0];
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray());
            return a.getComponentType();
        }
    }
}
