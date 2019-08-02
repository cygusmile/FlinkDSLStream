package com.ximalaya.flink.dsl.stream.calcite.flink.process.explode;

import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/8
 **/

public class QuerySimpleExplodeWithFilterProcessFunction
        extends QuerySimpleExplodeProcessFunction {

    protected final Evaluation filterCondition;

    public QuerySimpleExplodeWithFilterProcessFunction(Evaluation explodeFieldEvaluation,
                                                       Evaluation filterCondition,
                                                       String[] fields) {
        super(explodeFieldEvaluation,fields);
        this.filterCondition = filterCondition;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        filterCondition.open(getRuntimeContext());
    }

    @Override
    public void close() throws Exception {
        super.close();
        filterCondition.close();
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Row> out) throws Exception {
        Map<String,Object> value = convert(row,fields);
        if((boolean)filterCondition.eval(value)){
            Object a = explodeFieldEvaluation.eval(value);
            collect(a,-1,null,out);
        }
    }
}
