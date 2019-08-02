package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.api.field.decoder.NoneFieldDecoder
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import org.scalatest.FunSuite

/**
  *
  * @author martin.dong
  **/

class TestHDFSCreateBatchTableParser extends FunSuite{

  //good test create hdfs batch table

  //测试基本的建表逻辑 没有设定主键
  test("good test create hdfs batch table: simple") {

    val expected = new HDFSCreateBatchTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""),FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info", "extend")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY),
        SourceField.constructCreateField(None,"salary",None,Some(Double.box(10.2)),FieldType.DOUBLE)),
      key = None,
      source = "hdfs",
      keyDecoder = new NoneFieldDecoder,
      parallel = 10,
      optional = Map("path" → "/root/path/file",
        "fsDefaultName" → "hdfs://192.168.0.1",
        "zip" → "none",
        "dataType" → "json"
      ),Map(),Map()
    )
    val input =
      """
        |create batch table(
        |     userId as uid long 0,
        |     age int -1,
        |     info.address as address string '',
        |     info.extend.other as arr int_array [1,2,3,4],
        |     salary double 10.2
        |) required( source = 'hdfs',
        |            keyDecoder = 'none',
        |            parallel = '10'
        |           )
        |  optional( path = '/root/path/file',
        |            fsDefaultName  = 'hdfs://192.168.0.1',
        |            dataType = 'json',
        |            zip = 'none'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateBatchTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //测试基本的建表逻辑 设定主键
  test("good test create hdfs batch table: set primary key") {

    val expected = new HDFSCreateBatchTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"), Some(Long.box(0)),FieldType.LONG),
        SourceField.constructCreateField(None, "age", None, Some(Int.box(-1)),FieldType.INT),
        SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""),FieldType.STRING),
        SourceField.constructCreateField(Some(Array("info", "extend")), "other", Some("arr"), Some(Array(1,2,3,4)),FieldType.INT_ARRAY),
        SourceField.constructCreateField(None,"salaries",None,Some(Array(1.2f,3.2f,4.2f)),FieldType.FLOAT_ARRAY),
      SourceField.constructCreateField(None,"tip",None,Some(null),FieldType.BYTE)),
    key = Some("uid"),
      source = "hdfs",
      keyDecoder = new NoneFieldDecoder,
      parallel = 20,
      optional = Map("path" → "/root/path/file",
        "fsDefaultName" → "hdfs://192.168.0.1",
        "zip" → "none","dataType" → "json"
      ),Map(),Map()
    )
    val input =
      """
        |create batch table(
        |     userId as uid long 0,
        |     age int -1,
        |     info.address as address string '',
        |     info.extend.other as arr int_array [1,2,3,4],
        |     salaries float_array [1.2,3.2,4.2],
        |     tip byte null,
        |     key(uid)
        |) required( source = 'hdfs',
        |            keyDecoder = 'none',
        |            parallel = ' 20'
        |           )
        |   optional( path = ' /root/path/file',
        |             fsDefaultName  = 'hdfs://192.168.0.1',
        |             zip = 'none',dataType = 'json' ) as table1
        |
        """.stripMargin
    val parserResult = CreateBatchTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

}
