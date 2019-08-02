package com.ximalaya.flink.dsl.stream.api.message.encoder;

import com.ximalaya.flink.dsl.stream.type.InsertField;
import com.ximalaya.flink.dsl.stream.type.FieldType;
import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.function.Function;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/30
 **/

public class ZipMessageEncoder implements MessageEncoder {

    private MessageEncoder messageEncoder;
    private Function<byte[],byte[]> zipStrategy;

    public ZipMessageEncoder(MessageEncoder messageEncoder,
                             Function<byte[], byte[]> zipStrategy) {
        Preconditions.checkNotNull(messageEncoder);
        Preconditions.checkNotNull(zipStrategy);
        this.messageEncoder = messageEncoder;
        this.zipStrategy = zipStrategy;
    }

    @Override
    public byte[] encode(LinkedHashMap<String, Object> messages) {
        return zipStrategy.apply(messageEncoder.encode(messages));
    }

    @Override
    public void checkDefinitionFields(LinkedHashMap<String, InsertField> insertFieldMap)
            throws IllegalArgumentException {
        messageEncoder.checkDefinitionFields(insertFieldMap);
    }

    @Override
    public void open(LinkedHashMap<String, InsertField> insertFieldMap,
                     LinkedHashMap<String, FieldType> insertFieldTypeMap) {
        messageEncoder.open(insertFieldMap,insertFieldTypeMap);
    }

    @Override
    public String toString() {
        return "ZipMessageEncoder{" + messageEncoder.toString() +
                "," + zipStrategy.toString() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ZipMessageEncoder){
            ZipMessageEncoder other = ZipMessageEncoder.class.cast(obj);
            return other.messageEncoder.equals(this.messageEncoder) && other.zipStrategy.equals(this.zipStrategy);
        }else{
            return false;
        }
    }

    @Override
    public int hashCode() {
        return messageEncoder.hashCode()+zipStrategy.hashCode();
    }


}
