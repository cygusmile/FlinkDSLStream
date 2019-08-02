package com.ximalaya.flink.dsl.stream.calcite.flink.process.explode;

import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/8
 **/

public class QueryExplodeProcessFunction extends QuerySimpleExplodeProcessFunction {

    private List<Evaluation> queries;
    private int explodeFieldIndex;

    public QueryExplodeProcessFunction(Evaluation explodeFieldEvaluation,
                                       String[] fields,
                                       List<Evaluation> queries,
                                       int explodeFieldIndex) {
        super(explodeFieldEvaluation, fields);
        this.queries = queries;
        this.explodeFieldIndex = explodeFieldIndex;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        for (Evaluation query : queries) {
            query.open(getRuntimeContext());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (Evaluation query : queries) {
            query.close();
        }
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Row> out) throws Exception {
        Map<String, Object> value = convert(row, fields);
        Object a = explodeFieldEvaluation.eval(value);
        List<Object> list = queries.stream().map(query -> query.eval(value)).collect(Collectors.toList());
        collect(a,explodeFieldIndex,list, out);
    }
}
