package com.ximalaya.flink.dsl.stream.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/28
 **/

@Documented
@Target({ElementType.TYPE})
@Public
public @interface Internal {
}
