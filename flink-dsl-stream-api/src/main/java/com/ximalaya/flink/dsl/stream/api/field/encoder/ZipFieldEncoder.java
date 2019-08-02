package com.ximalaya.flink.dsl.stream.api.field.encoder;

import com.google.common.base.Preconditions;
import com.ximalaya.flink.dsl.stream.api.field.decoder.UnzipFieldDecoder;

import java.util.Map;
import java.util.function.Function;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/14
 **/

public class ZipFieldEncoder implements FieldEncoder {

    private FieldEncoder fieldEncoder;
    private Function<byte[],byte[]> zipStrategy;

    public ZipFieldEncoder(FieldEncoder fieldEncoder,
                             Function<byte[],byte[]> zipStrategy){
        Preconditions.checkNotNull(fieldEncoder);
        Preconditions.checkNotNull(zipStrategy);
        this.fieldEncoder = fieldEncoder;
        this.zipStrategy = zipStrategy;
    }

    @Override
    public byte[] encodeInt(int value) {
        return zipStrategy.apply(fieldEncoder.encodeInt(value));
    }

    @Override
    public byte[] encodeBoolean(boolean value) {
        return zipStrategy.apply(fieldEncoder.encodeBoolean(value));
    }

    @Override
    public byte[] encodeLong(long value) {
        return zipStrategy.apply(fieldEncoder.encodeLong(value));
    }

    @Override
    public byte[] encodeFloat(float value) {
        return zipStrategy.apply(fieldEncoder.encodeFloat(value));
    }

    @Override
    public byte[] encodeDouble(double value) {
        return zipStrategy.apply(fieldEncoder.encodeDouble(value));
    }

    @Override
    public byte[] encodeString(String value) {
        return zipStrategy.apply(fieldEncoder.encodeString(value));
    }

    @Override
    public byte[] encode(Object value) {
        return zipStrategy.apply(fieldEncoder.encode(value));
    }

    @Override
    public byte[] encodeStringArray(String[] value) {
        return zipStrategy.apply(fieldEncoder.encodeStringArray(value));
    }

    @Override
    public byte[] encodeObjectArray(Object[] value) {
        return zipStrategy.apply(fieldEncoder.encodeObjectArray(value));
    }

    @Override
    public byte[] encodeIntArray(int[] value) {
        return zipStrategy.apply(fieldEncoder.encodeIntArray(value));
    }

    @Override
    public byte[] encodeLongArray(long[] value) {
        return zipStrategy.apply(fieldEncoder.encodeLongArray(value));
    }

    @Override
    public byte[] encodeDoubleArray(double[] value) {
        return zipStrategy.apply(fieldEncoder.encodeDoubleArray(value));
    }

    @Override
    public byte[] encodeFloatArray(float[] value) {
        return zipStrategy.apply(fieldEncoder.encodeFloatArray(value));
    }

    @Override
    public byte[] encodeByteArray(byte[] value) {
        return zipStrategy.apply(fieldEncoder.encodeByteArray(value));
    }

    @Override
    public byte[] encodeMap(Map<Object, Object> value) {
        return zipStrategy.apply(fieldEncoder.encodeMap(value));
    }

    @Override
    public String toString() {
        return "ZipFieldEncoder{" + fieldEncoder.toString() + "," + zipStrategy.toString() + '}';
    }

    @Override
    public int hashCode() {
        return fieldEncoder.toString().hashCode()+zipStrategy.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof UnzipFieldDecoder){
            ZipFieldEncoder other = ZipFieldEncoder.class.cast(obj);
            return other.zipStrategy.equals(this.zipStrategy) && other.fieldEncoder.equals(this.fieldEncoder);
        }else{
            return false;
        }
    }
}
