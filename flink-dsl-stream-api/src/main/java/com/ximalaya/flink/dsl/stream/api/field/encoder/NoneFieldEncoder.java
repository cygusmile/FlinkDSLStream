package com.ximalaya.flink.dsl.stream.api.field.encoder;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/20
 **/

public class NoneFieldEncoder implements FieldEncoder {

    @Override
    public byte[] encodeInt(int value) {
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    @Override
    public byte[] encodeBoolean(boolean value) {
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    @Override
    public byte[] encodeLong(long value) {
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    @Override
    public byte[] encodeFloat(float value) {
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    @Override
    public byte[] encodeDouble(double value) {
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    @Override
    public byte[] encodeString(String value) {
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }
}
