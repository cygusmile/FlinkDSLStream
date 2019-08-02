package com.ximalaya.flink.dsl.stream.api.field.decoder;


import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.function.Function;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/30
 **/

public class UnzipFieldDecoder implements FieldDecoder {

    private FieldDecoder fieldDecoder;
    private Function<byte[],byte[]> unzipStrategy;

    public UnzipFieldDecoder(FieldDecoder fieldDecoder,
                             Function<byte[],byte[]> unzipStrategy){
        Preconditions.checkNotNull(fieldDecoder);
        Preconditions.checkNotNull(unzipStrategy);
        this.fieldDecoder = fieldDecoder;
        this.unzipStrategy = unzipStrategy;
    }


    @Override
    public int decodeInt(byte[] value) {
        return fieldDecoder.decodeInt(unzipStrategy.apply(value));
    }

    @Override
    public boolean decodeBool(byte[] value) {
        return fieldDecoder.decodeBool(unzipStrategy.apply(value));
    }

    @Override
    public long decodeLong(byte[] value) {
        return fieldDecoder.decodeLong(unzipStrategy.apply(value));
    }

    @Override
    public float decodeFloat(byte[] value) {
        return fieldDecoder.decodeFloat(unzipStrategy.apply(value));
    }

    @Override
    public double decodeDouble(byte[] value) {
        return fieldDecoder.decodeDouble(unzipStrategy.apply(value));
    }

    @Override
    public String decodeString(byte[] value) {
        return fieldDecoder.decodeString(unzipStrategy.apply(value));
    }

    @Override
    public Object decodeObject(byte[] value) {
        return fieldDecoder.decodeObject(unzipStrategy.apply(value));
    }

    @Override
    public String[] decodeStringArray(byte[] value) {
        return fieldDecoder.decodeStringArray(unzipStrategy.apply(value));
    }

    @Override
    public Object[] decodeObjectArray(byte[] value) {
        return fieldDecoder.decodeObjectArray(unzipStrategy.apply(value));
    }

    @Override
    public int[] decodeIntArray(byte[] value) {
        return fieldDecoder.decodeIntArray(unzipStrategy.apply(value));
    }

    @Override
    public long[] decodeLongArray(byte[] value) {
        return fieldDecoder.decodeLongArray(unzipStrategy.apply(value));
    }

    @Override
    public double[] decodeDoubleArray(byte[] value) {
        return fieldDecoder.decodeDoubleArray(unzipStrategy.apply(value));
    }

    @Override
    public float[] decodeFloatArray(byte[] value) {
        return fieldDecoder.decodeFloatArray(unzipStrategy.apply(value));
    }

    @Override
    public byte[] decodeByteArray(byte[] value) {
        return fieldDecoder.decodeByteArray(unzipStrategy.apply(value));
    }

    @Override
    public Map<Object, Object> decodeMap(byte[] value) {
        return fieldDecoder.decodeMap(unzipStrategy.apply(value));
    }

    @Override
    public String toString() {
        return "UnzipFieldDecoder{" + fieldDecoder.toString() + "," + unzipStrategy.toString() + '}';
    }

    @Override
    public int hashCode() {
        return fieldDecoder.toString().hashCode()+unzipStrategy.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof UnzipFieldDecoder){
            UnzipFieldDecoder other = UnzipFieldDecoder.class.cast(obj);
            return other.unzipStrategy.equals(this.unzipStrategy) && other.fieldDecoder.equals(this.fieldDecoder);
        }else{
            return false;
        }
    }
}
