package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import org.apache.calcite.sql.SqlKind;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EvaluationInfo {
    String name();
    String desc();
    SqlKind kind() default SqlKind.OTHER_FUNCTION;
}
