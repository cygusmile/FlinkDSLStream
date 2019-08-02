package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.functions.RuntimeContext;

import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.*;
import java.util.List;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/

public class ConditionalFunctions {

    private static Class<?> getReturnType(Map<Evaluation,Evaluation> conditions,Evaluation guard) {
        List<Class<?>> classes = Lists.newArrayList();
        for(Map.Entry<Evaluation,Evaluation> condition:conditions.entrySet()){
            condition.getKey().checkAndGetReturnType();
            classes.add(condition.getValue().checkAndGetReturnType());
        }
        if(guard!=null){
            classes.add(guard.checkAndGetReturnType());
        }
        return getCommonSupperClass(classes.toArray(new Class<?>[]{}));
    }

    @EvaluationInfo(name = "CASE",
            desc = "case operator",
            kind = SqlKind.CASE)
    public static class Case implements Evaluation{
        private Map<Evaluation,Evaluation> conditions;
        private Evaluation guard;

        public Case(Map<Evaluation,Evaluation> conditions,
                    Evaluation guard) {
            this.conditions = conditions;
            this.guard = guard;
        }

        @Override
        public Object eval(Map<String, Object> row) {
            for(Map.Entry<Evaluation,Evaluation> condition:conditions.entrySet()){
                if((boolean)condition.getKey().eval(row)){
                    return condition.getValue().eval(row);
                }
            }
            if(guard!=null){
                return guard.eval(row);
            }
            return null;
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(conditions.keySet().stream().
                    map(Evaluation::checkAndGetReturnType).
                    allMatch(c->c==Boolean.TYPE));
           return ConditionalFunctions.getReturnType(conditions,guard);
        }

        @Override
        public void open(RuntimeContext context) throws Exception {
            for(Map.Entry<Evaluation,Evaluation> condition:conditions.entrySet()) {
                condition.getKey().open(context);
                condition.getValue().open(context);
            }
            if(guard!=null){
                guard.open(context);
            }
        }
        @Override
        public void close() throws Exception {
            for(Map.Entry<Evaluation,Evaluation> condition:conditions.entrySet()) {
                condition.getKey().close();
                condition.getValue().close();
            }
            if(guard!=null){
                guard.close();
            }
        }
    }

    @EvaluationInfo(name = "NULLIF",
            desc = "nullif operator",
            kind = SqlKind.NULLIF)
    public static class NullIf extends BaseBinaryEvaluation{
        public NullIf(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if(a.equals(b)){
                return null;
            }
            return a;
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            right.checkAndGetReturnType();
            return left.checkAndGetReturnType();
        }
    }

    @EvaluationInfo(name = "COALESCE",
            desc = "coalesce operator",
            kind = SqlKind.COALESCE)
    public static class Coalesce extends BaseListEvaluation{
        public Coalesce(List<Evaluation> list) {
            super(list);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            for(Evaluation evaluation: list){
                Object a = evaluation.eval(row);
                if(a!=null){
                    return a;
                }
            }
            return null;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?>[] classes = new Class<?>[list.size()];
            for(int i = 0; i< list.size(); i++){
                classes[i] = list.get(i).checkAndGetReturnType();
            }
            return getCommonSupperClass(classes);
        }
    }
}

