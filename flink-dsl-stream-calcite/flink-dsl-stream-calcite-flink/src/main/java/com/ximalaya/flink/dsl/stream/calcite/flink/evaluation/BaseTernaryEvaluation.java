package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;


/**
 * ternary expression abstract interface
 * This expression has three input parameters
 */

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/4
 **/

public abstract class BaseTernaryEvaluation implements Evaluation{

    Evaluation left;
    Evaluation middle;
    Evaluation right;

    BaseTernaryEvaluation(Evaluation left, Evaluation middle, Evaluation right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    @Override
    public Evaluation eager() {
        this.left = left.eager();
        this.middle = middle.eager();
        this.right = right.eager();
        if(left instanceof Constant &&
                middle instanceof Constant && right instanceof Constant){
            return new Constant(eval(null));
        }else{
            return this;
        }
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        this.left = left.eager();
        this.middle = middle.eager();
        this.right = right.eager();
        if(left instanceof Constant &&
                middle instanceof Constant && right instanceof Constant){
            return new Constant(eval(row));
        }else{
            return this;
        }
    }

    @Override
    public void open(RuntimeContext context) throws Exception {
        left.open(context);
        middle.open(context);
        right.open(context);

    }

    @Override
    public void close() throws Exception{
        left.close();
        middle.close();
        right.close();
    }

    @Override
    public void accept(EvaluationVisitor visitor) {
        visitor.visit(this);
        visitor.visit(left);
        visitor.visit(middle);
        visitor.visit(right);
    }
}
