package com.ximalaya.flink.dsl.stream.api.message.encoder;

import com.ximalaya.flink.dsl.stream.annotation.Public;
import com.ximalaya.flink.dsl.stream.api.LifeCycle;
import com.ximalaya.flink.dsl.stream.type.InsertField;
import com.ximalaya.flink.dsl.stream.type.FieldType;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/28
 **/
@Public
public interface MessageEncoder extends Serializable, LifeCycle {

    /**
     * check definition fields is legal
     * @param  insertFieldMap definition fields
     * @throws IllegalArgumentException if definition fields is legal then throw exception
     */
    default void checkDefinitionFields(LinkedHashMap<String, InsertField> insertFieldMap) throws IllegalArgumentException {
    }

    /**
     * execute this function when init
     * @param insertFieldMap insert field info
     * @param insertFieldTypeMap insert field type info
     */
    default void open(LinkedHashMap<String, InsertField> insertFieldMap,
                      LinkedHashMap<String, FieldType> insertFieldTypeMap){

    }


    /**
     * encode message
     * @param messages messages
     * @return serialize message
     */
    byte[] encode(LinkedHashMap<String,Object> messages);
}
