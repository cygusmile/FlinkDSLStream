package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.calcite.runtime.SqlFunctions.*;
import static org.apache.calcite.runtime.SqlFunctions.le;
import static org.apache.calcite.runtime.SqlFunctions.toBigDecimal;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/

public class ComparisonFunctions {

    @EvaluationInfo(name = "=",
            desc = "'=' operator",
            kind = SqlKind.EQUALS)
    public static class Equals extends BaseBinaryEvaluation{
        public Equals(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if(a == null || b == null){
                return false;
            }
            return a.equals(b);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            left.checkAndGetReturnType();
            right.checkAndGetReturnType();
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS DISTINCT FROM",
            desc = "is distinct from operator",
            kind = SqlKind.IS_DISTINCT_FROM)
    public static class IsDistinctFrom extends BaseBinaryEvaluation{
        public IsDistinctFrom(Evaluation left, Evaluation right) {
           super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if(a == null){
                return false;
            }
            if(b == null){
                return true;
            }
            return a.equals(b);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            left.checkAndGetReturnType();
            right.checkAndGetReturnType();
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "<>",
            desc = "'<>' operator",
            kind = SqlKind.NOT_EQUALS)
    public static class NoEquals extends BaseBinaryEvaluation{
        public NoEquals(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if(a == null || b == null){
                return false;
            }
            return !a.equals(b);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            left.checkAndGetReturnType();
            right.checkAndGetReturnType();
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS NOT DISTINCT FROM",
            desc = "is not distinct from operator",
            kind = SqlKind.IS_NOT_DISTINCT_FROM)
    public static class IsNotDistinctFrom extends BaseBinaryEvaluation{
        public IsNotDistinctFrom(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if(a == null){
                return true;
            }
            if(b == null){
                return false;
            }
            return !a.equals(b);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            left.checkAndGetReturnType();
            right.checkAndGetReturnType();
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = ">",
            desc = "'>' operator",
            kind = SqlKind.GREATER_THAN)
    public static class GreatThan extends BaseBinaryEvaluation{
        public GreatThan(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object eval(Map<String, Object> row) {
            Object b0 = left.eval(row);
            Object b1 = right.eval(row);
            if(b0 == null || b1 == null){
                return false;
            }
            if (b0.getClass().equals(b1.getClass())
                    && b0 instanceof Comparable) {
                return ((Comparable) b0).compareTo(b1) > 0;
            }
            return gt(toBigDecimal((Number) b0), toBigDecimal((Number) b1));

        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument( a.equals(b) && a.isAssignableFrom(Comparable.class) ||
                    a.isPrimitive() && b.isPrimitive());
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = ">=",
            desc = "'>=' operator",
            kind = SqlKind.GREATER_THAN_OR_EQUAL)
    public static class GreatThanEquals extends BaseBinaryEvaluation{
        public GreatThanEquals(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @SuppressWarnings("unchecked")
        @Override
        public Object eval(Map<String, Object> row) {
            Object b0 = left.eval(row);
            Object b1 = right.eval(row);
            if(b0 == null || b1 == null){
                return false;
            }
            if (b0.getClass().equals(b1.getClass())
                    && b0 instanceof Comparable) {
                return ((Comparable) b0).compareTo(b1) >= 0;
            }
            return ge(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument( a.equals(b) && a.isAssignableFrom(Comparable.class) ||
                    a.isAssignableFrom(Number.class) && b.isAssignableFrom(Number.class));
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "<",
            desc = "'<' operator",
            kind = SqlKind.LESS_THAN)
    public static class LessThan extends BaseBinaryEvaluation{
        public LessThan(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @SuppressWarnings("unchecked")
        @Override
        public Object eval(Map<String, Object> row) {
            Object b0 = left.eval(row);
            Object b1 = right.eval(row);
            if(b0 == null || b1 == null){
                return false;
            }
            if (b0.getClass().equals(b1.getClass())
                    && b0 instanceof Comparable) {
                return ((Comparable) b0).compareTo(b1) < 0;
            }
            return lt(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument( a.equals(b) && a.isAssignableFrom(Comparable.class) ||
                    a.isAssignableFrom(Number.class) && b.isAssignableFrom(Number.class));
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "<=",
            desc = "'<' operator",
            kind = SqlKind.LESS_THAN_OR_EQUAL)
    public static class LessThanEquals extends BaseBinaryEvaluation{
        public LessThanEquals(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @SuppressWarnings("unchecked")
        @Override
        public Object eval(Map<String, Object> row) {
            Object b0 = left.eval(row);
            Object b1 = right.eval(row);
            if(b0 == null || b1 == null){
                return false;
            }
            if (b0.getClass().equals(b1.getClass())
                    && b0 instanceof Comparable) {
                return ((Comparable) b0).compareTo(b1) <= 0;
            }
            return le(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument( a.equals(b) && a.isAssignableFrom(Comparable.class) ||
                    a.isAssignableFrom(Number.class) && b.isAssignableFrom(Number.class));
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS NULL",
            desc = "is null operator",
            kind = SqlKind.IS_NULL)
    public static class IsNull extends BaseUnaryEvaluation{
        public IsNull(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return single.eval(row)==null;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            single.checkAndGetReturnType();
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS NOT NULL",
            desc = "is not null operator",
            kind = SqlKind.IS_NOT_NULL)
    public static class IsNotNull extends BaseUnaryEvaluation{
        public IsNotNull(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return single.eval(row)!=null;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            single.checkAndGetReturnType();
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "BETWEEN",
            desc = "between operator",
            kind = SqlKind.BETWEEN)
    public static class Between extends BaseTernaryEvaluation{
        private boolean includeBound;
        private boolean between;
        public Between(Evaluation left, Evaluation middle, Evaluation right,
                       boolean includeBound,boolean between) {
            super(left,middle,right);
            this.includeBound = includeBound;
            this.between = between;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = middle.eval(row);
            Object c = right.eval(row);
            if(b == null || c == null || a == null){
                return false;
            }
            if (b.getClass().equals(c.getClass()) &&
                    a.getClass().equals(c.getClass()) &&
                    b instanceof Comparable){
                if(((Comparable) b).compareTo(c) < 0){
                    return between == includeBound?
                            ((Comparable) a).compareTo(b) > 0 && ((Comparable) a).compareTo(c) < 0:
                            ((Comparable)a).compareTo(b) >= 0 && ((Comparable) a).compareTo(c) <= 0;
                }else if(((Comparable) b).compareTo(c) == 0){
                    return between == includeBound &&((Comparable) a).compareTo(b) == 0;
                }else{
                    return between == includeBound?
                            ((Comparable) a).compareTo(b) < 0 && ((Comparable) a).compareTo(c) > 0:
                            ((Comparable)a).compareTo(b) <= 0 && ((Comparable) a).compareTo(c) >= 0;
                }
            }
            BigDecimal aa = toBigDecimal((Number)a);
            BigDecimal bb = toBigDecimal((Number)b);
            BigDecimal cc= toBigDecimal((Number)c);
            if(lt(bb,cc)){
                return between == includeBound?gt(aa,bb) && lt(aa,cc):ge(aa,bb) && ge(aa,cc);
            }else if(eq(bb,cc)){
                return between == includeBound && eq(aa,bb);
            }else{
                return between == includeBound?lt(aa,bb) && gt(aa,cc):le(aa,bb) && ge(aa,cc);
            }
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = middle.checkAndGetReturnType();
            Class<?> c = right.checkAndGetReturnType();
            Preconditions.checkArgument( a.equals(b) && b.equals(c) && a.isAssignableFrom(Comparable.class) ||
                    a.isAssignableFrom(Number.class) && b.isAssignableFrom(Number.class) && c.isAssignableFrom(Number.class));
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IN",
            desc = "in operator",
            kind = SqlKind.IN)
    public static class In implements Evaluation{
        private Evaluation left;
        private List<Evaluation> rightList;
        public In(Evaluation left, List<Evaluation> rightList) {
            this.left = left;
            this.rightList = rightList;
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            if(a==null){
                return false;
            }
            return memberOf(a, rightList.stream().map(e->e.eval(row)).collect(Collectors.toList()));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            left.checkAndGetReturnType();
            for(Evaluation e:rightList){
                e.checkAndGetReturnType();
            }
            return Boolean.TYPE;
        }
        @Override
        public void open(RuntimeContext context) throws Exception{
            left.open(context);
            for(Evaluation e:rightList){
                e.open(context);
            }
        }
        @Override
        public void close() throws Exception {
            left.close();
            for(Evaluation e:rightList){
                e.close();
            }
        }
    }

    @EvaluationInfo(name = "NOT IN",
            desc = "no in operator",
            kind = SqlKind.NOT_IN)
    public static class NoIn implements Evaluation{
        private Evaluation left;
        private List<Evaluation> rightList;
        public NoIn(Evaluation left, List<Evaluation> rightList) {
            this.left = left;
            this.rightList = rightList;
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            if(a==null){
                return false;
            }
            return !memberOf(a, rightList.stream().map(e->e.eval(row)).collect(Collectors.toList()));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            left.checkAndGetReturnType();
            for(Evaluation e:rightList){
                e.checkAndGetReturnType();
            }
            return Boolean.TYPE;
        }
        @Override
        public void open(RuntimeContext context) throws Exception{
            left.open(context);
            for(Evaluation e:rightList){
                e.open(context);
            }
        }
        @Override
        public void close() throws Exception {
            left.close();
            for(Evaluation e:rightList){
                e.close();
            }
        }
    }
}
