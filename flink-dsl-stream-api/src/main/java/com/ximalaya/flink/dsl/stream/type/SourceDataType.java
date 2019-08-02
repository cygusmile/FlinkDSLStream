package com.ximalaya.flink.dsl.stream.type;


import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.api.message.decoder.CsvMessageDecoder;
import com.ximalaya.flink.dsl.stream.api.message.decoder.JsonMessageDecoder;
import com.ximalaya.flink.dsl.stream.api.message.decoder.MessageDecoder;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/19
 **/

public enum SourceDataType {

    /**
     * csv格式消息
     */
    CSV("csv"),
    /**
     * json格式消息
     */
    JSON("json");

    String dslExpress;

    public String getDslExpress() {
        return dslExpress;
    }

    SourceDataType(String dslExpress) {
        this.dslExpress = dslExpress;
    }

    public static Map<String, MessageDecoder> sourceDataTypeMap = Maps.newHashMap();

    static {
        sourceDataTypeMap.put(CSV.dslExpress,new CsvMessageDecoder());
        sourceDataTypeMap.put(JSON.dslExpress,new JsonMessageDecoder());
    }

    @SuppressWarnings("unchecked")
    public static MessageDecoder<Object> get(String dslExpress){
        return sourceDataTypeMap.get(dslExpress);
    }
}
