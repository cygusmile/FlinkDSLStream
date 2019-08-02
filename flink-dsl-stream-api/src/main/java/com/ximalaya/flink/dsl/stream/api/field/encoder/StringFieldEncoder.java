package com.ximalaya.flink.dsl.stream.api.field.encoder;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/4/29
 **/

public class StringFieldEncoder implements FieldEncoder {

    @Override
    public byte[] encodeInt(int value) {
        return String.valueOf(value).getBytes();
    }

    @Override
    public byte[] encodeBoolean(boolean value) {
        return String.valueOf(value).getBytes();
    }

    @Override
    public byte[] encodeLong(long value) {
        return String.valueOf(value).getBytes();
    }

    @Override
    public byte[] encodeFloat(float value) {
        return String.valueOf(value).getBytes();
    }

    @Override
    public byte[] encodeDouble(double value) {
        return String.valueOf(value).getBytes();
    }

    @Override
    public byte[] encodeString(String value) {
        return String.valueOf(value).getBytes();
    }

    @Override
    public String toString() {
        return "StringFieldEncoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof StringFieldEncoder;
    }
}
