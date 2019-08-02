package com.ximalaya.flink.dsl.stream.parser.create

import com.ximalaya.flink.dsl.stream.api.field.decoder.NoneFieldDecoder
import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import org.scalatest.FunSuite

/**
  *
  * @author martin.dong
  *
  **/

class TestMysqlCreateBatchTableParser extends FunSuite{

  //good test create mysql batch table

  //测试基本的建表逻辑 没有设定主键
  test("good test create mysql batch table: simple") {

    val expected = new MysqlCreateBatchTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"),None,FieldType.INT),
        SourceField.constructCreateField(None, "age", None,None,FieldType.INT),
        SourceField.constructCreateField(None, "address", Some("address"),None,FieldType.STRING),
        SourceField.constructCreateField(None, "other", Some("arr"),None,FieldType.STRING),
        SourceField.constructCreateField(None,"salary",None,None,FieldType.DOUBLE)),
      key = None,
      source = "mysql",
      keyDecoder = new NoneFieldDecoder,
      parallel = 10,
      optional = Map("jdbcUrl" → "192.168.0.1:2181,192.168.0.2:2182",
        "dbName" → "dbone",
        "tableName" → "tableone",
        "username"→"dong",
        "keyPattern"→"*",
        "password"→"xxx"
      ),Map(),Map()
    )
    val input =
      """
        |create batch table(
        |     userId as uid int,
        |     age int,
        |     address as address string,
        |     other as arr string,
        |     salary double
        |) required( source = 'mysql',
        |            keyDecoder = 'none',
        |            parallel = '10'
        |           )
        |  optional( jdbcUrl = '192.168.0.1:2181,192.168.0.2:2182',
        |            dbName  = 'dbone',keyPattern='*',
        |            tableName = 'tableone',
        |            username = 'dong',
        |            keyPattern = '*',
        |            password = 'xxx'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateBatchTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //测试基本的建表逻辑 设定主键
  test("good test create mysql batch table: set primary key") {

    val expected = new MysqlCreateBatchTableInfo("table1",
      fields = Seq(SourceField.constructCreateField(None, "userId", Some("uid"),None,FieldType.INT),
        SourceField.constructCreateField(None, "age", None,None,FieldType.INT),
        SourceField.constructCreateField(None, "address", Some("address"),None,FieldType.STRING),
        SourceField.constructCreateField(None, "other", Some("arr"),None,FieldType.STRING),
        SourceField.constructCreateField(None,"salaries",None,None,FieldType.DOUBLE),
        SourceField.constructCreateField(None,"tip",None,None,FieldType.INT)),
      key = Some("uid"),
      source = "mysql",
      keyDecoder = new NoneFieldDecoder,
      parallel = 20,
      optional = Map("jdbcUrl" → "192.168.0.1:2181,192.168.0.2:2182",
        "dbName" → "dbone",
        "tableName" → "tableone",
        "username"→"dong",
        "keyPattern"→"*",
        "password"→"xxx"
      ),Map(),Map()
    )
    val input =
      """
        |create batch table(
        |     userId as uid int,
        |     age int,
        |     address as address string,
        |     other as arr string,
        |     salaries double,
        |     tip int,
        |     key(uid)
        |) required( source = 'mysql',
        |            keyDecoder = 'none',
        |            parallel = ' 20'
        |           )
        |  optional( jdbcUrl = '192.168.0.1:2181,192.168.0.2:2182',
        |            dbName  = 'dbone',keyPattern='*',
        |            tableName = 'tableone',
        |            username = 'dong',
        |            keyPattern = '*',
        |            password = 'xxx'
        |           )as table1
        |
        """.stripMargin
    val parserResult = CreateBatchTableParser(input,Map(),Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

}
