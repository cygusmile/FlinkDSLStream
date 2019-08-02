package com.ximalaya.flink.dsl.stream.calcite.flink.process.explode;

import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/10
 **/

public class QueryWithFilterProcessFunction extends QueryProcessFunction {

    private final Evaluation filterCondition;
    public QueryWithFilterProcessFunction(String[] fields,
                                          List<Evaluation> queries,
                                          Evaluation filterCondition) {
        super(fields, queries);
        this.filterCondition = filterCondition;
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Row> out) throws Exception {
        Map<String, Object> value = convert(row, fields);
        if((boolean)filterCondition.eval(value)) {
            out.collect(Row.of(queries.stream().map(query -> query.eval(value)).toArray()));
        }
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
}
