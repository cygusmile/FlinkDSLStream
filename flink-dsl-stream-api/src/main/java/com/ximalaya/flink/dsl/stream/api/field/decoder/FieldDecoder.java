package com.ximalaya.flink.dsl.stream.api.field.decoder;

import com.ximalaya.flink.dsl.stream.annotation.Public;
import com.ximalaya.flink.dsl.stream.api.LifeCycle;
import com.ximalaya.flink.dsl.stream.type.FieldType;

import java.io.Serializable;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/29
 **/
@Public
public interface FieldDecoder extends Serializable, LifeCycle {

    /**
     * decode int
     * @param value byte[] value
     * @return int value
     */
    int decodeInt(byte[] value);

    /**
     * decode bool
     * @param value byte[] value
     * @return bool value
     */
    boolean decodeBool(byte[] value);


    default Object decode(byte[] value, FieldType fieldType){
        switch (fieldType){
            case FLOAT:return decodeFloat(value);
            case STRING:return decodeString(value);
            case LONG:return decodeLong(value);
            case BOOL:return decodeBool(value);
            case INT:return decodeInt(value);
            case MAP:return decodeMap(value);
            case DOUBLE:return decodeDouble(value);
            case OBJECT:return decodeObject(value);
            case INT_ARRAY:return decodeIntArray(value);
            case LONG_ARRAY:return decodeLongArray(value);
            case FLOAT_ARRAY:return decodeFloatArray(value);
            case DOUBLE_ARRAY:return decodeObjectArray(value);
            case STRING_ARRAY:return decodeStringArray(value);
            case OBJECT_ARRAY:return decodeObjectArray(value);
            case BYTE_ARRAY:return decodeByteArray(value);
            default:throw new RuntimeException("unknown field type: "+fieldType);
        }
    }
    /**
     * decode long
     * @param value byte[] value
     * @return long value
     */
    long decodeLong(byte[] value);

    /**
     * decode float
     * @param value byte[] value
     * @return float value
     */
    float decodeFloat(byte[] value);

    /**
     * decode double
     * @param value byte[] value
     * @return double value
     */
    double decodeDouble(byte[] value);

    /**
     * decode string
     * @param value byte[] value
     * @return string value
     */
    String decodeString(byte[] value);

    /**
     * decode object
     * @param value byte[] value
     * @return object value
     */
    default Object decodeObject(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode string[]
     * @param value byte[] value
     * @return string[] value
     */
    default String[] decodeStringArray(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode object[]
     * @param value byte[] value
     * @return object[] value
     */
    default Object[] decodeObjectArray(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode int[]
     * @param value byte[] value
     * @return int[] value
     */
    default int[] decodeIntArray(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode long[]
     * @param value byte[] value
     * @return long[] value
     */
    default long[] decodeLongArray(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode double[]
     * @param value byte[] value
     * @return double[] value
     */
    default double[] decodeDoubleArray(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode float[]
     * @param value byte[] value
     * @return float[] value
     */
    default float[] decodeFloatArray(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }

    /**
     * decode byte[]
     * @param value byte[] value
     * @return byte[] value
     */
    default byte[] decodeByteArray(byte[] value){
        return value;
    }

    /**
     * decode map
     * @param value byte[] value
     * @return map value
     */
    default Map<Object,Object> decodeMap(byte[] value){
        throw new UnsupportedOperationException("doest not support decode!");
    }



}
