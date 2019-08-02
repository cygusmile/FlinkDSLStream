package com.ximalaya.flink.dsl.stream.api;

import com.ximalaya.flink.dsl.stream.annotation.Public;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/20
 **/
@Public
public interface LifeCycle {

    /**
     *the initialization of an expression before it is executed
     * @param context runtime context
     * @throws Exception an exception that may be thrown after the expression is open
     */
    default void open(RuntimeContext context) throws Exception{
    }

    /**
     * the closing operation after an expression completes execution
     *@throws Exception an exception that may be thrown after the expression is closed
     */
    default void close() throws Exception{
    }

}
