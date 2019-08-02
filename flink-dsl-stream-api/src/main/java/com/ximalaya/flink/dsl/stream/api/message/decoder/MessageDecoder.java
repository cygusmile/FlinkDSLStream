package com.ximalaya.flink.dsl.stream.api.message.decoder;

import com.ximalaya.flink.dsl.stream.annotation.Public;
import com.ximalaya.flink.dsl.stream.api.LifeCycle;
import com.ximalaya.flink.dsl.stream.type.SourceField;
import scala.Option;

import java.io.Serializable;
import java.util.*;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/27
 **/
@Public
public interface MessageDecoder<V> extends Serializable, LifeCycle {

    /**
     * check definition fields is legal
     * @param fields definition fields
     * @throws IllegalArgumentException if definition fields is legal then throw exception
     */
    default void checkDefinitionFields(List<SourceField> fields) throws IllegalArgumentException {
    }

    /**
     * deserialize message
     * @param message raw binary message
     * @return format message
     */
    V decode(byte[] message);


    /**
     *resolve int by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved int value
     */
    Option<Integer> resolveInt(SourceField sourceField,
                               V message);

    /**
     *resolve bool by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved bool value
     */
    Option<Boolean> resolveBoolean(SourceField sourceField,
                                     V message);

    /**
     *resolve long by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved long value
     */
    Option<Long> resolveLong(SourceField sourceField,
                             V message);

    /**
     *resolve float by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved float value
     */
    Option<Float> resolveFloat(SourceField sourceField,
                                 V message);

    /**
     *resolve double by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved double value
     */
    Option<Double> resolveDouble(SourceField sourceField,
                                 V message);

    /**
     *resolve string by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved string value
     */
    Option<String> resolveString(SourceField sourceField,
                                   V message);

    /**
     *resolve object by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved object value
     */
    default Option<Object> resolveObject(SourceField sourceField,
                                           V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve int[] by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved int[] value
     */
    default Option<int[]> resolveIntArray(SourceField sourceField,
                                            V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve long[] by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved long[] value
     */
    default Option<long[]> resolveLongArray(SourceField sourceField,
                                              V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve float[] by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved float[] value
     */
    default Option<float[]> resolveFloatArray(SourceField sourceField,
                                                V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve double[] by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved double[] value
     */
    default Option<double[]> resolveDoubleArray(SourceField sourceField,
                                                  V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve string[] by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved string[] value
     */
    default Option<String[]> resolveStringArray(SourceField sourceField,
                                                  V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve object[] by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved object[] value
     */
    default Option<Object[]> resolveObjectArray(SourceField sourceField,
                                                  V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }

    /**
     *resolve map by sourceField metaInfo
     * @param sourceField source field
     * @param message format  message
     * @return resolved map value
     */
    default Option<Map<Object,Object>> resolveMap(SourceField sourceField,
                                                    V message){
        throw new UnsupportedOperationException("doest not support resolve!");
    }
}
