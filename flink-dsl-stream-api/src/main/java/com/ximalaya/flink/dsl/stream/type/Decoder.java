package com.ximalaya.flink.dsl.stream.type;

import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.api.field.decoder.FieldDecoder;
import com.ximalaya.flink.dsl.stream.api.field.decoder.NoneFieldDecoder;
import com.ximalaya.flink.dsl.stream.api.field.decoder.RawFieldDecoder;
import com.ximalaya.flink.dsl.stream.api.field.decoder.StringFieldDecoder;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/19
 **/

public enum Decoder {
    /**
     * 针对原生数据的解码
     */
    RAW("raw"),
    /**
     * 针对字符串数据的解码
     */
    STRING("string"),

    NONE("none");

    String dslExpress;

    public String getDslExpress() {
        return dslExpress;
    }

    Decoder(String dslExpress){
        this.dslExpress = dslExpress;
    }

    public static Map<String, FieldDecoder> decodes = Maps.newHashMap();

    static {
        decodes.put(RAW.dslExpress,new RawFieldDecoder());
        decodes.put(STRING.dslExpress,new StringFieldDecoder());
        decodes.put(NONE.dslExpress,new NoneFieldDecoder());
    }

    public static FieldDecoder get(String dslExpress){
        return decodes.get(dslExpress);
    }
}
