package com.ximalaya.flink.dsl.stream.calcite.flink.process.explode;

import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.Evaluation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/10
 **/

public class QueryProcessFunction extends ProcessFunction<Row,Row> {

    protected final String[] fields;
    protected final List<Evaluation> queries;

    protected final Map<String,Object> convert(Row row, String[] fields){
        Map<String,Object> value = Maps.newHashMap();
        for(int i=0;i<fields.length;i++){
            value.put(fields[i],row.getField(i));
        }
        return value;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        for (Evaluation query : queries) {
            query.open(getRuntimeContext());
        }
    }

    @Override
    public void close() throws Exception {
        for (Evaluation query : queries) {
            query.close();
        }
    }

    public QueryProcessFunction(String[] fields, List<Evaluation> queries) {
        this.fields = fields;
        this.queries = queries;
    }


    @Override
    public void processElement(Row row, Context ctx, Collector<Row> out) throws Exception {
        Map<String, Object> value = convert(row, fields);
        out.collect(Row.of(queries.stream().map(query -> query.eval(value)).toArray()));
    }
}
