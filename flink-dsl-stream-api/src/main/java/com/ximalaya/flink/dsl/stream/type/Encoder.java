package com.ximalaya.flink.dsl.stream.type;

import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder;
import com.ximalaya.flink.dsl.stream.api.field.encoder.RawFieldEncoder;
import com.ximalaya.flink.dsl.stream.api.field.encoder.StringFieldEncoder;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/19
 **/

public enum Encoder {
    /**
     * 针对原生数据的解码
     */
    RAW("raw"),
    /**
     * 针对字符串数据的解码
     */
    STRING("string");

    String dslExpress;

    public String getDslExpress() {
        return dslExpress;
    }

    Encoder(String dslExpress){
        this.dslExpress = dslExpress;
    }

    public static Map<String, FieldEncoder> decodes = Maps.newHashMap();

    static {
        decodes.put(RAW.dslExpress,new RawFieldEncoder());
        decodes.put(STRING.dslExpress,new StringFieldEncoder());
    }

    public static FieldEncoder get(String dslExpress){
        return decodes.get(dslExpress);
    }
}
