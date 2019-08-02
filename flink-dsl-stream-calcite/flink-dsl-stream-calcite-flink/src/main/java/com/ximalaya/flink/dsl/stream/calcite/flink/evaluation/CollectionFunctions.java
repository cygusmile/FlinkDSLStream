package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;
import java.util.Map;
import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.*;
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/

public class CollectionFunctions {


    @EvaluationInfo(name = "CARDINALITY",
            desc = "returns the number of elements in array or map",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Cardinality extends BaseUnaryEvaluation {
        public Cardinality(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object object = single.eval(row);
            if (isArray(object)) {
                if( object.getClass().getComponentType() == Integer.TYPE) {
                    return ((int[]) object).length;
                }
                if( object.getClass().getComponentType() == Float.TYPE){
                    return ((float[])object).length;
                }
                if( object.getClass().getComponentType() == Double.TYPE){
                    return ((double[])object).length;
                }
                if( object.getClass().getComponentType() == Long.TYPE){
                    return ((long[])object).length;
                }
                return ((Object[])object).length;
            }
            return ((Map)(object)).size();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray() || a == Map.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "ITEM",
            desc = "returns the element at position integer in array. The right starts from 1",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Item extends BaseBinaryEvaluation {
        public Item(Evaluation left, Evaluation right) {
            super(left,right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object object = left.eval(row);
            if (isArray(object)) {
                return ((Object[]) object)[(int) this.right.eval(row)];
            }
            return ((Map) left.eval(row)).get(this.right.eval(row));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = right.checkAndGetReturnType();
            Class<?> b = left.checkAndGetReturnType();
            Preconditions.checkArgument( a.isArray() && b == Integer.TYPE ||
                    a == Map.class);
            if (a.isArray()) {
                return a.getComponentType();
            }
            return Object.class;
        }
    }

    @EvaluationInfo(name = "ELEMENT",
            desc = "returns the element at position integer in array. The right starts from 1",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Element extends BaseUnaryEvaluation {
        public Element(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object[] object = (Object[]) single.eval(row);
            if (object.length == 0) {
                return null;
            }
            if (object.length == 1) {
                return object[0];
            }
            throw new RuntimeException("array has more one element size:" + object.length);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a.isArray());
            return a.getComponentType();
        }
    }
}
