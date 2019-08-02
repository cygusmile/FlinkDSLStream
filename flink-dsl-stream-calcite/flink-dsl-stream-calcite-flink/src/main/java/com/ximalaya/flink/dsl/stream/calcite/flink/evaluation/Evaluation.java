package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.ximalaya.flink.dsl.stream.api.LifeCycle;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import java.util.Map;

/**
 * an abstract interface describing an executable expression that fully
 * describes the behavior that the interface can represent.
 * It has a computational method that can execute the expression.
 * It has a lifecycle interface, etc.
 */

/**
 * @author martin.dong
 **/
public interface Evaluation extends Serializable, LifeCycle {

    /**
     * pass a row of data into an expression and evaluate the data in that row .
     * eg. if the expression is a function evaluate the function
     * eg. if the expression is a constant then return the constant value directly
     * eg. if the expression is a variable then get the data of the variable
     * @param row a row of data
     * @return return result of evaluation
     */
    Object eval(Map<String, Object> row);

    /**
     *  check the input parameters and gets the return parameter of the expression
     *  corresponding to the input parameter list and
     * returns null if the expression does not support the input parameter list
     * @return return type
     */
    Class<?> checkAndGetReturnType();

    /**
     * pre-compute the constant expression in an expression,
     * but it will not be calculated in advance
     * if there is a custom function in the expression.
     *
     * for example
     *
     * before pre-compute valuation
     *                       and
     *                   /       \
     *                  /           \
     *                 /               \
     *                /                   \
     *           isGood(score,lastScore)  isBad(3,2)  <--- const expression ( eg. can pre-compute)
     *
     * after pre-compute evaluation
     *
     *                           and
     *                        /       \
     *                       /           \
     *                      /               \
     *                     /                   \
     *                isGood(score,lastScore)  true
     * @return perform a computed expression on a constant expression
     */
    default Evaluation eager() {
        return this;
    }

    /**
     * pre-compute the variable expression in an expression,
     * but it will not be calculated in advance
     * if there is a custom function in the expression.
     *
     * for example
     *
     * before pre-compute valuation
     *                       and
     *                   /       \
     *                  /           \
     *                 /               \
     *                /                   \
     *           isGood(score,lastScore)  isBad(uid,2)  <--- variable expression ( eg. can pre-compute if uid value had given in row)
     *
     * after pre-compute evaluation
     *
     *                           and
     *                        /       \
     *                       /           \
     *                      /               \
     *                     /                   \
     *                isGood(score,lastScore)  true
     * @return perform a computed expression on a variable expression
     */
    default Evaluation eager(Map<String,Object> row){
        return this;
    }

    /**
     * used in visitor design pattern to implement the function of importing a visitor to achieve the access of evaluation
     * @param visitor the implement of visitor
     */
    default void accept(EvaluationVisitor visitor){
        visitor.visit(this);
    }

}
