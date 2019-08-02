package com.ximalaya.flink.dsl.stream.api.message.encoder;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/29
 **/

public class CsvMessageEncoder implements MessageEncoder {

    @Override
    public byte[] encode(LinkedHashMap<String, Object> messages) {
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String,Object> entry:messages.entrySet()){
            builder.append(entry.getValue().toString());
            builder.append(",");
        }
        return builder.substring(0,builder.length()-1).getBytes();
    }

    @Override
    public String toString() {
        return "CsvMessageEncoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CsvMessageEncoder;
    }
}
