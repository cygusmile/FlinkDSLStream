package com.ximalaya.flink.dsl.stream.api.field.decoder;

import com.ximalaya.flink.dsl.stream.utils.StringUtils;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/29
 **/

public class StringFieldDecoder implements FieldDecoder {

    @Override
    public int decodeInt(byte[] value) {
        return Integer.parseInt(StringUtils.fromBytes(value));
    }

    @Override
    public boolean decodeBool(byte[] value) {
        return Boolean.parseBoolean(StringUtils.fromBytes(value));
    }

    @Override
    public long decodeLong(byte[] value) {
        return Long.parseLong(StringUtils.fromBytes(value));
    }

    @Override
    public float decodeFloat(byte[] value) {
        return Float.parseFloat(StringUtils.fromBytes(value));
    }

    @Override
    public double decodeDouble(byte[] value) {
        return Double.parseDouble(StringUtils.fromBytes(value));
    }

    @Override
    public String decodeString(byte[] value) {
        return StringUtils.fromBytes(value);
    }

    @Override
    public String toString() {
        return "StringFieldDecoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof StringFieldDecoder;
    }
}
