package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/15
 **/

public interface EvaluationVisitor {
    /**
     * used in visitor design pattern to implement an evaluation to
     * implement the function of accessing the evaluation
     * @param evaluation the implement of evaluation
     */
    void visit(Evaluation evaluation);
}
