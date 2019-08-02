package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;


/**
 * binary expression abstract interface
 * This expression has two input parameters
 */
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/4
 **/

public abstract class BaseBinaryEvaluation implements Evaluation{

    Evaluation left;
    Evaluation right;

    BaseBinaryEvaluation(Evaluation left, Evaluation right) {
        this.left = left;
        this.right = right;
    }


    @Override
    public Evaluation eager() {
        this.left = left.eager();
        this.right = right.eager();
        if(left instanceof Constant && right instanceof Constant){
            return new Constant(eval(null));
        }else{
            return this;
        }
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        this.left = left.eager(row);
        this.right = right.eager(row);
        if(left instanceof Constant && right instanceof Constant){
            return new Constant(eval(row));
        }else{
            return this;
        }
    }

    @Override
    public void open(RuntimeContext context) throws Exception{
        left.open(context);
        right.open(context);
    }

    @Override
    public void close() throws Exception{
        left.close();
        right.close();
    }

    @Override
    public void accept(EvaluationVisitor visitor) {
        visitor.visit(left);
        visitor.visit(right);
    }
}
