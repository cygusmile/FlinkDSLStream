package com.ximalaya.flink.dsl.stream.udf.annotation;


import java.lang.annotation.*;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/21
 **/
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface UdfInfo {
    String name();
    String desc();
    String constructFields() default "";
}
