package com.ximalaya.flink.dsl.stream.api.message.encoder;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.utils.StringUtils;
import org.junit.Test;

import java.util.LinkedHashMap;
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/28
 **/

public class TestSinkDataType {

    @Test
    public void testJsonMessage() throws Exception{
        Class<?> clazz = Class.forName("com.ximalaya.flink.dsl.stream.api.message.encoder.JsonMessageEncoder");
        MessageEncoder messageEncoder = MessageEncoder.class.cast(clazz.newInstance());

        LinkedHashMap<String,Object> message = Maps.newLinkedHashMap();
        message.put("age",23);
        message.put("city","shanghai");
        message.put("code",new int[]{1,2,3});

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("age",23);
        jsonObject.put("city","shanghai");
        jsonObject.put("code",new int[]{1,2,3});

        assert StringUtils.fromBytes(messageEncoder.encode(message)).equals(jsonObject.toJSONString());
    }

    @Test
    public void testCsvMessage() throws Exception{
        Class<?> clazz = Class.forName("com.ximalaya.flink.dsl.stream.api.message.encoder.CsvMessageEncoder");
        MessageEncoder messageEncoder = MessageEncoder.class.cast(clazz.newInstance());

        LinkedHashMap<String,Object> message = Maps.newLinkedHashMap();
        message.put("age",23);
        message.put("city","shanghai");
        message.put("code",12.3);
        assert StringUtils.fromBytes(messageEncoder.encode(message)).equals("23,shanghai,12.3");
    }
}
