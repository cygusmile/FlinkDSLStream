package com.ximalaya.flink.dsl.stream.utils;

import java.nio.charset.Charset;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/27
 **/

public class StringUtils {

    private StringUtils(){

    }

    public static String fromBytes(byte[] bytes){
        return new String(bytes, Charset.forName("UTF-8"));
    }
}
