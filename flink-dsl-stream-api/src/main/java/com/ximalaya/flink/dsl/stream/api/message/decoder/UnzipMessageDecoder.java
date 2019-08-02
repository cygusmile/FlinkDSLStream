package com.ximalaya.flink.dsl.stream.api.message.decoder;

import com.ximalaya.flink.dsl.stream.type.SourceField;
import com.google.common.base.Preconditions;
import scala.Option;

import java.util.*;
import java.util.function.Function;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/30
 **/

public class UnzipMessageDecoder<V> implements MessageDecoder<V> {

    private MessageDecoder<V> messageDecoder;
    private Function<byte[],byte[]> unzipStrategy;

    public UnzipMessageDecoder(MessageDecoder<V> messageDecoder,
                               Function<byte[],byte[]> unzipStrategy){
        Preconditions.checkNotNull(messageDecoder);
        Preconditions.checkNotNull(unzipStrategy);
        this.messageDecoder = messageDecoder;
        this.unzipStrategy = unzipStrategy;
    }

    @Override
    public void checkDefinitionFields(List<SourceField> fields) throws IllegalArgumentException {
        messageDecoder.checkDefinitionFields(fields);
    }

    @Override
    public V decode(byte[] message) {
        return messageDecoder.decode(unzipStrategy.apply(message));
    }

    @Override
    public Option<Integer> resolveInt(SourceField sourceField,
                                      V message) {
        return messageDecoder.resolveInt(sourceField,message);
    }

    @Override
    public Option<Boolean> resolveBoolean(SourceField sourceField,
                                            V message) {
        return messageDecoder.resolveBoolean(sourceField,message);
    }

    @Override
    public Option<Long> resolveLong(SourceField sourceField,
                                    V message) {
        return messageDecoder.resolveLong(sourceField,message);
    }

    @Override
    public Option<Float> resolveFloat(SourceField sourceField,
                                        V message) {
        return messageDecoder.resolveFloat(sourceField,message);
    }

    @Override
    public Option<Double> resolveDouble(SourceField sourceField, V message) {
        return messageDecoder.resolveDouble(sourceField,
                message);
    }

    @Override
    public Option<String> resolveString(SourceField sourceField,
                                          V message) {
        return messageDecoder.resolveString(sourceField,message);
    }

    @Override
    public Option<Object> resolveObject(SourceField sourceField,
                                          V message) {
        return messageDecoder.resolveObject(sourceField,message);
    }

    @Override
    public Option<int[]> resolveIntArray(SourceField sourceField, V message) {
        return messageDecoder.resolveIntArray(sourceField,message);
    }

    @Override
    public Option<long[]> resolveLongArray(SourceField sourceField, V message) {
        return messageDecoder.resolveLongArray(sourceField,message);
    }

    @Override
    public Option<float[]> resolveFloatArray(SourceField sourceField, V message) {
        return messageDecoder.resolveFloatArray(sourceField,message);
    }

    @Override
    public Option<double[]> resolveDoubleArray(SourceField sourceField, V message) {
        return messageDecoder.resolveDoubleArray(sourceField,message);
    }

    @Override
    public Option<String[]> resolveStringArray(SourceField sourceField, V message) {
        return messageDecoder.resolveStringArray(sourceField,message);
    }

    @Override
    public Option<Object[]> resolveObjectArray(SourceField sourceField, V message) {
        return messageDecoder.resolveObjectArray(sourceField,message);
    }

    @Override
    public Option<Map<Object, Object>> resolveMap(SourceField sourceField, V message) {
        return messageDecoder.resolveMap(sourceField,message);
    }

    @Override
    public String toString() {
        return "UnzipMessageDecoder{" + messageDecoder.toString() +
                "," + unzipStrategy.toString() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof UnzipMessageDecoder){
            UnzipMessageDecoder other = UnzipMessageDecoder.class.cast(obj);
            return other.messageDecoder.equals(this.messageDecoder) && other.unzipStrategy.equals(this.unzipStrategy);
        }else{
            return false;
        }
    }

    @Override
    public int hashCode() {
        return messageDecoder.hashCode()+unzipStrategy.hashCode();
    }
}