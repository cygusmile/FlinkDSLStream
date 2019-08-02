package com.ximalaya.flink.dsl.stream.calcite.flink.process.explode;

import com.ximalaya.flink.dsl.stream.calcite.flink.context.QueryRuntimeContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/8
 **/

public class QueryProcessFunctionFactory {
    public static ProcessFunction<Row, Row> createQueryProcessFunction(QueryRuntimeContext context) {
        if (context.getExplodeFieldEvaluation().isPresent()) {
            if (context.getFilterEvaluation().isPresent()) {
                if (!context.getSelectEvaluations().isEmpty()) {
                    return new QueryExplodeWithFilterProcessFunction(context.getExplodeFieldEvaluation().get().getValue(),
                            context.getFilterEvaluation().get(),
                            context.getSourceNames(), context.getSelectEvaluations(),
                            context.getExplodeFieldEvaluation().get().getKey());
                } else {
                    return new QuerySimpleExplodeWithFilterProcessFunction(context.getExplodeFieldEvaluation().get().getValue(),
                            context.getFilterEvaluation().get(), context.getSourceNames());
                }
            } else {
                if (!context.getSelectEvaluations().isEmpty()) {
                    return new QueryExplodeProcessFunction(context.getExplodeFieldEvaluation().get().getValue(),
                            context.getSourceNames(),context.getSelectEvaluations(),context.getExplodeFieldEvaluation().get().getKey());
                } else {
                    return new QuerySimpleExplodeProcessFunction(context.getExplodeFieldEvaluation().get().getValue(),
                            context.getSourceNames());

                }
            }
        } else {
            if (context.getFilterEvaluation().isPresent()) {
                return new QueryWithFilterProcessFunction(context.getSourceNames(),
                        context.getSelectEvaluations(),
                        context.getFilterEvaluation().get());
            } else {
                return new QueryProcessFunction(context.getSourceNames(), context.getSelectEvaluations());
            }
        }
    }
}
