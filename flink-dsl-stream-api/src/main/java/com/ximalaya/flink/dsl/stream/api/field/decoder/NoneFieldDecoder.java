package com.ximalaya.flink.dsl.stream.api.field.decoder;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/20
 **/

public class NoneFieldDecoder implements FieldDecoder {

    @Override
    public int decodeInt(byte[] value) {
        throw new UnsupportedOperationException("doest not support decode!");
    }

    @Override
    public boolean decodeBool(byte[] value) {
        throw new UnsupportedOperationException("doest not support decode!");
    }

    @Override
    public long decodeLong(byte[] value) {
        throw new UnsupportedOperationException("doest not support decode!");
    }

    @Override
    public float decodeFloat(byte[] value) {
        throw new UnsupportedOperationException("doest not support decode!");
    }

    @Override
    public double decodeDouble(byte[] value) {
        throw new UnsupportedOperationException("doest not support decode!");
    }

    @Override
    public String decodeString(byte[] value) {
        throw new UnsupportedOperationException("doest not support decode!");
    }


    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoneFieldDecoder;
    }

    @Override
    public String toString() {
        return "NoneFieldDecoder";
    }
}
