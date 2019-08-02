package com.ximalaya.flink.dsl.stream.type;

import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.api.message.encoder.CsvMessageEncoder;
import com.ximalaya.flink.dsl.stream.api.message.encoder.JsonMessageEncoder;
import com.ximalaya.flink.dsl.stream.api.message.encoder.MessageEncoder;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/19
 **/

public enum SinkDataType {

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

    SinkDataType(String dslExpress) {
        this.dslExpress = dslExpress;
    }

    public static Map<String, MessageEncoder> sinkDataTypes = Maps.newHashMap();

    static {
        sinkDataTypes.put(CSV.dslExpress,new CsvMessageEncoder());
        sinkDataTypes.put(JSON.dslExpress,new JsonMessageEncoder());
    }

    public static MessageEncoder get(String dslExpress){
        return sinkDataTypes.get(dslExpress);
    }
}
