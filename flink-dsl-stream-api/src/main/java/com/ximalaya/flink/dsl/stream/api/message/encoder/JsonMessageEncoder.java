package com.ximalaya.flink.dsl.stream.api.message.encoder;

import com.alibaba.fastjson.JSONObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/28
 **/

public class JsonMessageEncoder implements MessageEncoder {

    @Override
    public byte[] encode(LinkedHashMap<String, Object> messages) {
        JSONObject result = new JSONObject();
        for(Map.Entry<String,Object> entry:messages.entrySet()){
            String fieldName=entry.getKey();
            Object fieldValue=entry.getValue();
            result.put(fieldName,fieldValue);
        }
        return result.toJSONString().getBytes();
    }

    @Override
    public String toString() {
        return "JsonMessageEncoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JsonMessageEncoder;
    }
}
