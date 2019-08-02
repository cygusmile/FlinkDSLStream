package com.ximalaya.flink.dsl.stream.api.field.decoder;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/29
 **/

public class RawFieldDecoder implements FieldDecoder {

    @Override
    public int decodeInt(byte[] value) {
        return Bytes.toInt(value);
    }

    @Override
    public boolean decodeBool(byte[] value) {
        return Bytes.toBoolean(value);
    }

    @Override
    public long decodeLong(byte[] value) {
        return Bytes.toLong(value);
    }

    @Override
    public float decodeFloat(byte[] value) {
        return Bytes.toFloat(value);
    }

    @Override
    public double decodeDouble(byte[] value) {
        return Bytes.toDouble(value);
    }

    @Override
    public String decodeString(byte[] value) {
        return Bytes.toString(value);
    }

    @Override
    public String toString() {
        return "RawFieldDecoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RawFieldDecoder;
    }
}
