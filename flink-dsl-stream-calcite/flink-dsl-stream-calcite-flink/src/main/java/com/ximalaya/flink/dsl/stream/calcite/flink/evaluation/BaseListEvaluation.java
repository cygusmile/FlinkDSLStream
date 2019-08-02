package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 *list expression abstract interface
 * This expression has multiple input parameters
 */
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/4
 **/

public abstract class BaseListEvaluation implements Evaluation {

    List<Evaluation> list;

    BaseListEvaluation(List<Evaluation> list){
        this.list = list;
    }

    @Override
    public Evaluation eager() {
        this.list = list.stream().map(Evaluation::eager).collect(Collectors.toList());
        if (this.list.stream().allMatch(eager -> eager instanceof Constant)) {
            return new Constant(eval(null));
        } else {
            return this;
        }
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        this.list = list.stream().map(e->e.eager(row)).collect(Collectors.toList());
        if (this.list.stream().allMatch(eager -> eager instanceof Constant)) {
            return new Constant(eval(row));
        } else {
            return this;
        }
    }

    @Override
    public void close() throws Exception{
        for(Evaluation e:list){
            e.close();
        }
    }

    @Override
    public void open(RuntimeContext context)throws Exception {
        for(Evaluation e:list){
            e.open(context);
        }
    }

    @Override
    public void accept(EvaluationVisitor visitor) {
        visitor.visit(this);
        list.forEach(visitor::visit);
    }
}
