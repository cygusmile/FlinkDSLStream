package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.`type`.Decoder
import com.ximalaya.flink.dsl.stream.api.message.decoder.JsonMessageDecoder
import org.scalatest.FunSuite
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.side.{Ignore, Stop}
/**
  *
  * @author martin.dong
  *
  **/

class TestRabbitmqCreateStreamTableParser extends FunSuite{

  //good test create rabbitmq stream table

  //测试基本的建表逻辑 没有设定主键和水位
  test("good test create rabbitmq stream table: simple") {

    val expected = new RabbitMQCreateStreamTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"),Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"),Some(""), FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info", "extend")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY),
        SourceField.constructCreateField(None,"salary",None,Some(Double.box(10.2)),FieldType.DOUBLE)),
      key = None,
      waterMarkStrategy = None,
      dataType = new JsonMessageDecoder,
      keyDecoder = Decoder.decodes.get(Decoder.NONE.getDslExpress),
      parallel = 10,
      castError = Stop,
      optional = Map("host" → "192.168.0.1",
        "port" → "9238",
        "queue" → "test-queue",
        "useCorrelationIds" → "true"
      )
    )
    val input =
      """
        |create stream table(
        |     userId as uid long 0,
        |     age int -1,
        |     info.address as address string '',
        |     info.extend.other as arr int_array [1,2,3,4],
        |     salary double 10.2
        |) required( source = 'rabbitmq',
        |            dataType = 'json',
        |            keyDecoder = 'none',
        |            parallel = '10',
        |            castError = 'stop' )
        |  optional( host = '192.168.0.1',
        |            port  = '9238',
        |            queue = 'test-queue',
        |            useCorrelationIds = 'true'
        |           )as table1
        |
        """.stripMargin

    val parserResult = CreateStreamTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

  //测试基本的建表逻辑 设定主键
  test("good test create rabbitmq stream table: set primary key") {

    val expected = new RabbitMQCreateStreamTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"),Some(""), FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info", "extend")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY),
        SourceField.constructCreateField(None,"salaries",None,Some(Array(1.2f,3.2f,4.2f)),FieldType.FLOAT_ARRAY),
        SourceField.constructCreateField(None,"tip",None,Some(null),FieldType.BYTE)),
      key = Some("xid"),
      waterMarkStrategy = None,
      dataType = new JsonMessageDecoder,
      keyDecoder = Decoder.decodes.get(Decoder.STRING.getDslExpress),
      parallel = 20,
      castError = Ignore,
      optional = Map("host" → "192.168.0.1",
        "port" → "9238",
        "queue" → "test-queue",
        "useCorrelationIds" → "true"
      )
    )
    val input =
      """
        |create stream table(
        |     userId as uid long 0,
        |     age int -1,
        |     info.address as address string '',
        |     info.extend.other as arr int_array [1,2,3,4],
        |     salaries float_array [1.2,3.2,4.2],
        |     tip byte null,
        |     key(xid)
        |) required( source = 'rabbitmq',
        |            dataType = 'json ',
        |            keyDecoder = 'string',
        |            parallel = ' 20',
        |            castError = ' ignore' )
        |  optional( host = '192.168.0.1',
        |            port  = '9238',
        |            queue = 'test-queue',
        |            useCorrelationIds = 'true'
        |           )
        |as table1
        |
        """.stripMargin
    val parserResult = CreateStreamTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

  //测试基本的建表逻辑 设定水位
  test("good test create rabbitmq stream table: set waterMark") {

    val expected = new RabbitMQCreateStreamTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, None,FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"),Some(""), FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info", "extend")), "timeStamp", Some("ts"),Some(Long.box(-1)), FieldType.LONG),
        SourceField.constructCreateField(Some(Array("info", "extend")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY)),
      key = Some("xid"),
      waterMarkStrategy = Some(new MaxOuterOfOrder("ts", 100)),
      dataType = new JsonMessageDecoder,
      keyDecoder = Decoder.decodes.get(Decoder.STRING.getDslExpress),
      parallel = 10,
      castError = Stop,
      optional = Map("host" → "192.168.0.1",
        "port" → "9238",
        "queue" → "test-queue",
        "useCorrelationIds" → "true"
      )
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
        |) required( source = 'rabbitmq',
        |            dataType = 'json',
        |            keyDecoder = 'string',
        |            parallel = '10',
        |            castError = 'stop' )
        |       optional( host = '192.168.0.1',
        |            port  = '9238',
        |            queue = 'test-queue',
        |            useCorrelationIds = 'true'
        |           )as table1
        |
        """.stripMargin
    val parserResult = CreateStreamTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }
}
