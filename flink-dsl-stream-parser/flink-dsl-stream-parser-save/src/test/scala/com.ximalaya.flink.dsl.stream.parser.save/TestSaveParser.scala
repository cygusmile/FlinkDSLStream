package com.ximalaya.flink.dsl.stream.parser.save

import com.ximalaya.fink.dsl.stream.parser.save.{DummySaveTableInfo, HBaseSaveTableInfo, Nothing, SaveParser}
import com.ximalaya.flink.dsl.stream.`type`.Encoder
import com.ximalaya.flink.dsl.stream.parser.SinkField
import org.scalatest.FunSuite

/**
  *
  * @author martin.dong
  *
  **/

class TestSaveParser extends FunSuite{

  //good test save hbase table

  //测试基本的建表逻辑
  test("good test create hbase dimension table: simple") {

    val expected = new HBaseSaveTableInfo("table1",
      fields = Seq(SinkField("age", Some(List("info")),"age"),
        SinkField("address", Some(List("info")),"addr"),SinkField("name", Some(List("base")),"fullName")),
      key =Some("uid"),
      cacheStrategy = Nothing,
      keyEncoder = Encoder.get(Encoder.STRING.getDslExpress),
      parallel = 2,
      optional = Map("zookeeper" → "192.168.0.1:2181,192.168.0.2:2182",
        "tableName" → "test-hbase",
        "encoder"→"string"
      ),
      dynamicEncoders = Map("string"→Encoder.get(Encoder.STRING.getDslExpress)),dynamicSinkDataType = Map()
    )
    val input =
      """
        |
        |save table1(
        |   age as info.age,
        |   address as info.addr,
        |   name as base.fullName,
        |   cache = none,
        |   key(uid)
        |) required( sink = 'hbase',
        |            keyEncoder = 'string',
        |            parallel = '2'
        |           )
        |  optional( zookeeper = '192.168.0.1:2181,192.168.0.2:2182 ',
        |            tableName  = 'test-hbase',
        |            encoder = 'string'
        |           )
        |
        """.stripMargin

    val parserResult = SaveParser(input,Map("string"→Encoder.get(Encoder.STRING.getDslExpress)),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //good test save dummy table

  //测试基本的建表逻辑
  test("good test create dummy dimension table: simple") {

    val expected = new DummySaveTableInfo("table1",
      fields = Seq(SinkField("age", Some(List("info")),"age"),
        SinkField("address", Some(List("info")),"addr"),SinkField("name", Some(List("base")),"fullName")),
      cacheStrategy = Nothing,
      key =None,
      keyEncoder = Encoder.get(Encoder.STRING.getDslExpress),
      parallel = 2,
      optional = Map(),
      dynamicEncoders = Map(),
      dynamicSinkDataType = Map()
    )
    val input =
      """
        |
        |save table1(
        |   age as info.age,
        |   address as info.addr,
        |   name as base.fullName,
        |   cache = none
        |) required( sink = 'dummy',
        |            keyEncoder = 'string',
        |            parallel = '2'
        |           )
        |  optional(debug='true')
        |
        """.stripMargin

    val parserResult = SaveParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

}
