package com.ximalaya.flink.dsl.stream.api.field.encoder;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Objects;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/14
 **/

public class RawFieldEncoder implements FieldEncoder {

    @Override
    public byte[] encodeInt(int value) {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] encodeBoolean(boolean value) {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] encodeLong(long value) {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] encodeFloat(float value) {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] encodeDouble(double value) {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] encodeString(String value) {
        return Bytes.toBytes(value);
    }

    @Override
    public String toString() {
        return "RawFieldEncoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RawFieldEncoder;
    }
}

