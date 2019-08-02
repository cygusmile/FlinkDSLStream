package com.ximalaya.flink.dsl.stream.api.field.encoder;

import com.ximalaya.flink.dsl.stream.annotation.Public;
import com.ximalaya.flink.dsl.stream.api.LifeCycle;

import java.io.Serializable;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/4/28
 **/
@Public
public interface FieldEncoder extends Serializable, LifeCycle {

    /**
     * encodeInt int
     * @param value int value
     * @return int value
     */
    byte[] encodeInt(int value);

    /**
     * encodeInt boolean
     * @param value boolean value
     * @return bool value
     */
    byte[] encodeBoolean(boolean value);

    /**
     * encodeInt long
     * @param value long value
     * @return long value
     */
    byte[] encodeLong(long value);

    /**
     * encodeInt float
     * @param value float value
     * @return float value
     */
    byte[] encodeFloat(float value);

    /**
     * encodeInt double
     * @param value double value
     * @return double value
     */
    byte[] encodeDouble(double value);

    /**
     * encodeInt string
     * @param value string value
     * @return string value
     */
    byte[] encodeString(String value);


    /**
     * encodeInt object
     * @param value object value
     * @return object value
     */

    default byte[] encodeObject(Object value){
        throw new UnsupportedOperationException("doest not support encodeObject!");
    }

    /**
     * encodeInt object
     * @param value object value
     * @return object value
     */
    @SuppressWarnings("unchecked")
    default byte[] encode(Object value){
        Class<?> clazz = value.getClass();
        if(clazz == Integer.class){
            return encodeInt((int)value);
        }
        if(clazz == Boolean.class){
            return encodeBoolean((boolean)value);
        }
        if(clazz == Long.class){
            return encodeLong((long) value);
        }
        if(clazz == Float.class){
            return encodeFloat((float) value);
        }
        if(clazz == Double.class){
            return encodeDouble((double)value);
        }
        if(clazz == String.class){
            return encodeString((String)value);
        }
        if(clazz.isArray()) {
            Class<?> componentClazz = clazz.getComponentType();
            if(componentClazz == Integer.TYPE){
                return encodeIntArray((int[])value);
            }
            if(componentClazz == Float.TYPE){
                return encodeFloatArray((float[])value);
            }
            if(componentClazz == Double.TYPE){
                return encodeDoubleArray((double[])value);
            }
            if(componentClazz == Long.TYPE){
                return encodeLongArray((long[])value);
            }
            if(componentClazz == String.class){
                return encodeStringArray((String[])value);
            }
            if(componentClazz == Byte.TYPE){
                return encodeByteArray((byte[])value);
            }
            if(componentClazz == Object.class){
                return encodeObjectArray((Object[])value);
            }
        }
        if(clazz == Map.class){
            return encodeMap((Map)value);
        }
        return encodeObject(value);
    }

    /**
     * encodeInt string[]
     * @param value string[] value
     * @return byte[] value
     */
    default byte[] encodeStringArray(String[] value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    /**
     * encodeInt object[]
     * @param value byte[] value
     * @return object[] value
     */
    default byte[] encodeObjectArray(Object[] value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    /**
     * encodeInt int[]
     * @param value byte[] value
     * @return int[] value
     */
    default byte[] encodeIntArray(int[] value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    /**
     * encodeInt long[]
     * @param value byte[] value
     * @return long[] value
     */
    default byte[] encodeLongArray(long[] value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    /**
     * encodeInt double[]
     * @param value byte[] value
     * @return double[] value
     */
    default byte[] encodeDoubleArray(double[] value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    /**
     * encodeInt float[]
     * @param value byte[] value
     * @return float[] value
     */
    default byte[] encodeFloatArray(float[] value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }

    /**
     * encodeInt byte[]
     * @param value byte[] value
     * @return byte[] value
     */
    default byte[] encodeByteArray(byte[] value){
        return value;
    }

    /**
     * encodeInt map
     * @param value byte[] value
     * @return map value
     */
    default byte[] encodeMap(Map<Object,Object>  value){
        throw new UnsupportedOperationException("doest not support encodeInt!");
    }
}
