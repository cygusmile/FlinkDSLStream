package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.`type`.Decoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.JsonMessageDecoder
import com.ximalaya.flink.dsl.stream.api.field.decoder.UnzipFieldDecoder
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.side.Stop
import com.ximalaya.flink.dsl.stream.parser._
import org.scalatest.FunSuite
/**
  *
  * @author martin.dong
  *
  **/

class TestKafkaCreateStreamTableParser extends FunSuite {

  //good test create kafka stream table

  //测试基本的建表逻辑 没有设定主键和水位
  test("good test create kafka stream table: simple") {

    val expected = new KafkaCreateStreamTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""),FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info","extend")),"other",Some("arr"),Some(Array(1,2,3,4)),FieldType.INT_ARRAY)),
      key = None,
      waterMarkStrategy = None,
      dataType = new JsonMessageDecoder,
      keyDecoder = Decoder.decodes.get(Decoder.NONE.getDslExpress),
      parallel = 10,
      castError = Stop,
      optional = Map("topic" → "test-topic",
        "zookeeper" → "zkInfo",
        "broker" → "brokerInfo",
        "offset" → "1000",
        "groupId" → "test-group")
    )
    val input =
      """
        |create stream table(
        |     userId as uid long 0,
        |     age int -1,
        |     info.address as address string '',
        |     info.extend.other as arr int_array [1,2,3,4]
        |) required( source = 'kafka',
        |            dataType = 'json',
        |            keyDecoder = 'none',
        |            parallel = '10',
        |            castError = 'stop' )
        |  optional( topic = 'test-topic',
        |            zookeeper = 'zkInfo',
        |            broker = 'brokerInfo',
        |            offset = '1000',
        |            groupId = 'test-group'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateStreamTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

  //测试基本的建表逻辑 设定主键
  test("good test create kafka stream table: set primary key") {

    val expected = new KafkaCreateStreamTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""),FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info","extend")),"other",Some("arr"),Some(Array(1,2,3,4)),FieldType.INT_ARRAY)),
      key = Some("xid"),
      waterMarkStrategy = None,
      dataType = new JsonMessageDecoder,
      keyDecoder = new UnzipFieldDecoder(Decoder.decodes.get(Decoder.STRING.getDslExpress),"gzip".toUnzipStrategy),
      parallel = 10,
      castError = Stop,
      optional = Map("topic" → "test-topic",
        "zookeeper" → "zkInfo",
        "broker" → "brokerInfo",
        "offset" → "1000",
        "groupId" → "test-group")
    )
    val input =
      """
        |create stream table(
        |     userId as uid long 0,
        |     age int -1,
        |     info.address as address string '',
        |     info.extend.other as arr int_array [1,2,3,4],
        |     key(xid)
        |) required( source = 'kafka',
        |            dataType = 'json',
        |            keyDecoder = 'unzip(string,gzip)',
        |            parallel = '10',
        |            castError = 'stop' )
        |  optional( topic = 'test-topic',
        |            zookeeper = 'zkInfo',
        |            broker = 'brokerInfo',
        |            offset = '1000',
        |            groupId = 'test-group'
        |           ) as table1
        |
        """.stripMargin
    val parserResult = CreateStreamTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

  //测试基本的建表逻辑 设定水位
  test("good test create kafka stream table: set waterMark") {

    val expected = new KafkaCreateStreamTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, None,FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""),FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info", "extend")), "timeStamp", Some("ts"), Some(Long.box(-1)),FieldType.LONG),
        SourceField.constructCreateField(Some(Array("info","extend")),"other",Some("arr"),Some(Array(1,2,3,4)),FieldType.INT_ARRAY)),
      key = Some("xid"),
      waterMarkStrategy = Some(new MaxOuterOfOrder("ts", 100)),
      dataType = new JsonMessageDecoder,
      keyDecoder = Decoder.decodes.get(Decoder.STRING.getDslExpress),
      parallel = 10,
      castError = Stop,
      optional = Map("topic" → "test-topic",
        "zookeeper" → "zkInfo",
        "broker" → "brokerInfo",
        "offset" → "1000",
        "groupId" → "test-group")
    )
    val input =
      """
        |create stream table(
        |     userId as uid long 0,
        |     age int,
        |     info.address as address string '',
        |     info.extend.timeStamp as ts long -1,
        |      info.extend.other as arr int_array [1,2,3,4],
        |     key (xid),
        |     waterMark = maxOuterOfOder (ts , 100 )
        |) required( source = 'kafka',
        |            dataType = 'json',
        |            keyDecoder = 'string',
        |            parallel = '10',
        |            castError = 'stop' )
        |  optional( topic = 'test-topic',
        |            zookeeper = 'zkInfo',
        |            broker = 'brokerInfo',
        |            offset = '1000',
        |            groupId = 'test-group'
        |           )as table1
        |
        """.stripMargin
    val parserResult = CreateStreamTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }
}
