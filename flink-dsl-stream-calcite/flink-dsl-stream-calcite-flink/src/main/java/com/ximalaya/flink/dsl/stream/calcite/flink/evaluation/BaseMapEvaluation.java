package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/4
 **/

public abstract class BaseMapEvaluation implements Evaluation {

    Map<Evaluation, Evaluation> map;

    BaseMapEvaluation(Map<Evaluation, Evaluation> map) {
        this.map = map;
    }

    @Override
    public void open(RuntimeContext context) throws Exception{
        for(Map.Entry<Evaluation,Evaluation> entry:map.entrySet()){
            entry.getKey().open(context);
            entry.getValue().open(context);
        }
    }

    @Override
    public void close() throws Exception{
        for(Map.Entry<Evaluation,Evaluation> entry:map.entrySet()){
            entry.getKey().close();;
            entry.getValue().close();
        }
    }

    @Override
    public Evaluation eager() {
        this.map = map.entrySet().stream().map(kv->
           Pair.of(kv.getKey().eager(),kv.getValue().eager())
        ).collect(Collectors.toMap(Pair::getKey,Pair::getValue));
        if(this.map.entrySet().stream().allMatch(pair->pair.getKey()
                instanceof Constant && pair.getValue() instanceof Constant)){
            return new Constant(eval(null));
        }else{
            return this;
        }
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        this.map = map.entrySet().stream().map(kv->
                Pair.of(kv.getKey().eager(row),kv.getValue().eager(row))
        ).collect(Collectors.toMap(Pair::getKey,Pair::getValue));
        if(this.map.entrySet().stream().allMatch(pair->pair.getKey()
                instanceof Constant && pair.getValue() instanceof Constant)){
            return new Constant(eval(row));
        }else{
            return this;
        }
    }

    @Override
    public void accept(EvaluationVisitor visitor) {
        visitor.visit(this);
        map.forEach((k,v)->{
            visitor.visit(k);
            visitor.visit(v);
        });
    }
}
