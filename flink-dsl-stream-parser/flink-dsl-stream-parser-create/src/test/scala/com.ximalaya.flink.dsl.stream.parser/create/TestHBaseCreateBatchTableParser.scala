package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.api.field.decoder.{NoneFieldDecoder, StringFieldDecoder}
import org.scalatest.FunSuite
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
/**
  *
  * @author martin.dong
  *
  **/
class TestHBaseCreateBatchTableParser extends FunSuite{

  //good test create hdfs batch table

  //测试基本的建表逻辑 没有设定主键
  test("good test create hbase batch table: simple") {

    val expected = new HBaseCreateBatchTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(Some(Array("info")), "userId", Some("uid"),Some(Long.box(0)), FieldType.LONG),
        SourceField.constructCreateField(Some(Array("info")), "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""),FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY),
        SourceField.constructCreateField(Some(Array("info")),"salary",None,Some(Double.box(10.2)),FieldType.DOUBLE)),
      key = None,
      source = "hbase",
      keyDecoder = new NoneFieldDecoder,
      parallel = 10,
      optional = Map("zookeeper" → "192.168.0.1:2181,192.168.0.2:2182",
        "tableName" → "test-hbase",
        "keyPattern" → "*",
        "decoder"→"string"
      ),Map(),Map()
    )
    val input =
      """
        |create batch table(
        |     info.userId as uid long 0,
        |     info.age int -1,
        |     info.address as address string '',
        |     info.other as arr int_array [1,2,3,4],
        |     info.salary double 10.2
        |) required( source = 'hbase',
        |            keyDecoder = 'none',
        |            parallel = '10'
        |           )
        |  optional( zookeeper = '192.168.0.1:2181,192.168.0.2:2182 ',
        |            tableName  = 'test-hbase',keyPattern='*',
        |            decoder = 'string'
        |           )as table1
        |
        """.stripMargin

    val parserResult = CreateBatchTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //测试基本的建表逻辑 设定主键
  test("good test create hbase batch table: set primary key") {

    val expected = new HBaseCreateBatchTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(Some(Array("info")), "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(Some(Array("info")), "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"),Some(""), FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY),
        SourceField.constructCreateField(Some(Array("info")),"salaries",None,Some(Array(1.2f,3.2f,4.2f)),FieldType.FLOAT_ARRAY),
        SourceField.constructCreateField(Some(Array("info")),"tip",None,Some(null),FieldType.BYTE)),
      key = Some("uid"),
      source = "hbase",
      keyDecoder = new StringFieldDecoder,
      parallel = 20,
      optional = Map("zookeeper" → "192.168.0.1:2181,192.168.0.2:2182",
        "tableName" → "test-hbase",
        "keyPattern" → "*",
        "decoder"→"string"
      ),Map(),Map()
    )
    val input =
      """
        |create batch table(
        |     info.userId as uid long 0,
        |     info.age int -1,
        |     info.address as address string '',
        |     info.other as arr int_array [1,2,3,4],
        |     info.salaries float_array [1.2,3.2,4.2],
        |     info.tip byte null,
        |     key(uid)
        |) required( source = 'hbase',
        |            keyDecoder = 'string',
        |            parallel = ' 20'
        |           )
      optional( zookeeper = '192.168.0.1:2181,192.168.0.2:2182 ',
 |            tableName  = 'test-hbase',keyPattern='*',
 |            decoder = 'string'
 |           )  as table1
        |
        """.stripMargin
    val parserResult = CreateBatchTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

}
