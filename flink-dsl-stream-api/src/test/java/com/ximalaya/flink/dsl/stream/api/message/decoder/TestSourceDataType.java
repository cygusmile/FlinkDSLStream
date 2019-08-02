package com.ximalaya.flink.dsl.stream.api.message.decoder;

import com.ximalaya.flink.dsl.stream.type.FieldType;
import com.ximalaya.flink.dsl.stream.type.SourceField;
import org.junit.Test;
import scala.Option;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/27
 **/

public class TestSourceDataType {

    @SuppressWarnings("unchecked")
    @Test
    public void testCsvMessage() throws Exception{
        Class<?> clazz=Class.forName("com.ximalaya.flink.dsl.stream.api.message.decoder.CsvMessageDecoder");
        MessageDecoder messageDecoder = MessageDecoder.class.cast(clazz.newInstance());
        Object result= messageDecoder.decode("shanghai,20,true".getBytes());

        SourceField stringCreateField = SourceField.constructSimpleCreateField("0",
                Option.apply("city"),Option.empty(), FieldType.STRING);
        SourceField integerCreateField = SourceField.constructSimpleCreateField("1",
                Option.apply("age"),Option.empty(),FieldType.STRING);
        SourceField booleanCreateField = SourceField.constructSimpleCreateField("2",
                Option.apply("marry"),Option.empty(),FieldType.STRING);

        assert messageDecoder.resolveInt(integerCreateField,result).equals(Option.apply(20));
        assert messageDecoder.resolveBoolean(booleanCreateField,result).equals(Option.apply(true));
        assert messageDecoder.resolveString(stringCreateField,result).equals(Option.apply("shanghai"));
    }

}
