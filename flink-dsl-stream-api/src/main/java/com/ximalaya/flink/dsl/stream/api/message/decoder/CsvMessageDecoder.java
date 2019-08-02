package com.ximalaya.flink.dsl.stream.api.message.decoder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.type.SourceField;
import com.ximalaya.flink.dsl.stream.utils.StringUtils;
import scala.Function1;
import scala.Option;

import java.util.*;
import java.util.function.Function;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/27
 **/

public class CsvMessageDecoder implements MessageDecoder<Map<String,String>> {

    @Override
    public Map<String, String> decode(byte[] message) {
        Preconditions.checkNotNull(message);
        String[] fields= StringUtils.fromBytes(message).split(",");
        Map<String,String> result = Maps.newHashMap();
        for(int i=0;i<fields.length;i++){
            result.put(String.valueOf(i),fields[i]);
        }
        return result;
    }

    private <T> Option<T> resolve(Option<String> option, Function<String,T> function){
            if(option.isDefined()){
                return Option.apply(function.apply(option.get()));
            }else{
                return Option.empty();
            }
    }

    @Override
    public Option<Integer> resolveInt(SourceField sourceField, Map<String,String> message) {
        return resolve(Option.apply(message.get(sourceField.getFieldName())),Integer::valueOf);
    }

    @Override
    public Option<Boolean> resolveBoolean(SourceField sourceField, Map<String,String> message) {
        return resolve(Option.apply(message.get(sourceField.getFieldName())),Boolean::valueOf);
    }

    @Override
    public Option<Long> resolveLong(SourceField sourceField, Map<String,String> message) {
        return resolve(Option.apply(message.get(sourceField.getFieldName())),Long::valueOf);
    }

    @Override
    public Option<Float> resolveFloat(SourceField sourceField, Map<String,String> message) {
        return resolve(Option.apply(message.get(sourceField.getFieldName())),Float::valueOf);
    }

    @Override
    public Option<Double> resolveDouble(SourceField sourceField, Map<String,String> message) {
        return resolve(Option.apply(message.get(sourceField.getFieldName())),Double::valueOf);
    }

    @Override
    public Option<String> resolveString(SourceField sourceField, Map<String, String> message) {
        return Option.apply(message.get(sourceField.getFieldName()));
    }

    @Override
    public String toString() {
        return "CsvMessageDecoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CsvMessageDecoder;
    }
}
