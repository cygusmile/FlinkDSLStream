package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;


import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;


/**
 * unary expression abstract interface
 * This expression has one input parameters
 */
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/4
 **/

public abstract class BaseUnaryEvaluation implements Evaluation {

    Evaluation single;

    BaseUnaryEvaluation(Evaluation single) {
        this.single = single;
    }

    @Override
    public Evaluation eager() {
        this.single = single.eager();
        if(this.single instanceof Constant){
            return new Constant(eval(null));
        }else{
            return this;
        }
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        this.single = single.eager(row);
        if(this.single instanceof Constant){
            return new Constant(eval(row));
        }else{
            return this;
        }
    }

    @Override
    public void open(RuntimeContext context) throws Exception{
        single.open(context);
    }

    @Override
    public void close() throws Exception {
        single.close();
    }

    @Override
    public void accept(EvaluationVisitor visitor) {
        visitor.visit(this);
        visitor.visit(single);
    }
}
