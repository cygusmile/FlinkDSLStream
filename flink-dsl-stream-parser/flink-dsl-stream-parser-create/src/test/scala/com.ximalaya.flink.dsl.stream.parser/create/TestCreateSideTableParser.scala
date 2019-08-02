package com.ximalaya.flink.dsl.stream.parser.create

import java.util.concurrent.TimeUnit

import com.ximalaya.flink.dsl.stream.`type`.{FieldType, SourceField}
import com.ximalaya.flink.dsl.stream.api.field.decoder.StringFieldDecoder
import com.ximalaya.flink.dsl.stream.api.field.encoder.StringFieldEncoder
import com.ximalaya.flink.dsl.stream.side._
import com.ximalaya.flink.dsl.stream.parser._
import org.scalatest.FunSuite

/**
  *
  * @author martin.dong
  *
  **/

class TestCreateSideTableParser extends FunSuite {

  //good test create hbase dimension table

  //测试基本的建表逻辑
  test("good test create hbase side table: simple") {

    val hbaseSideInfo = HBaseSideInfo("test-hbase", "192.168.0.1:2181,192.168.0.2:2182", new StringFieldEncoder, new StringFieldDecoder)
    val physicsSchema = Seq(SourceField.constructCreateField(Some(Array("info")), "age", None, Some(Int.box(-1)), FieldType.INT),
      SourceField.constructCreateField(Some(Array("info")), "address", Some("address"), Some(""), FieldType.STRING),
      SourceField.constructCreateField(Some(Array("info")), "salary", None, Some(Double.box(10.2)), FieldType.DOUBLE))

    val expected = SideTableInfo("table1", hbaseSideInfo, CreateSideTableInfo.toLogicSchema(physicsSchema), "uid", All("*", 100, TimeUnit.SECONDS), physicsSchema.toJavaList, Stop)
    val input =
      """
        |create side table(
        |     info.age int -1,
        |     info.address as address string '',
        |     info.salary double 10.2,
        |     key(uid),
        |     cache = all('*',100,s)
        |) required( source = 'hbase',
        |            keyEncoder = 'string',
        |            castError = 'stop'
        |           )
        |  optional( zookeeper = '192.168.0.1:2181,192.168.0.2:2182 ',
        |            tableName  = 'test-hbase',
        |            decoder = 'string'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateSideTableParser(input, Map(), Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //good test create mysql dimension table
  //测试基本的建表逻辑
  test("good test create mysql side table: simple") {

    val mysqlSideInfo = MysqlSideInfo("tableone", "dbone", "192.168.0.1:2181,192.168.0.2:2182", "dong", "xxx")
    val physicsSchema = Seq(SourceField.constructSimpleCreateField("uid", None, None, FieldType.INT),
      SourceField.constructSimpleCreateField("address", Some("address"), None, FieldType.STRING),
      SourceField.constructSimpleCreateField("salary", None, None, FieldType.DOUBLE))

    val expected = SideTableInfo("table1", mysqlSideInfo, CreateSideTableInfo.toLogicSchema(physicsSchema), "uid", Nothing, physicsSchema.toJavaList, Stop)
    val input =
      """
        |create side table(
        |     uid int,
        |     address as address string,
        |     salary double,
        |     key(uid),
        |     cache = none
        |) required( source = 'mysql',
        |            keyEncoder = 'none',
        |            castError = 'stop'
        |           )
        |   optional( jdbcUrl = '192.168.0.1:2181,192.168.0.2:2182',
        |            dbName  = 'dbone',
        |            tableName = 'tableone',
        |            username = 'dong',
        |            password = 'xxx'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateSideTableParser(input, Map(), Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //good test create hdfs dimension table

  //测试基本的建表逻辑
  test("good test create hdfs side table: simple") {

    val hdfsSideInfo = HDFSSideInfo("/root/path/file", "hdfs://192.168.0.1", ZipStrategy.none, FileType.Json)
    val physicsSchema = Seq(SourceField.constructSimpleCreateField("uid", None, None, FieldType.INT),
      SourceField.constructSimpleCreateField("address", Some("address"), None, FieldType.STRING),
      SourceField.constructSimpleCreateField("salary", None, None, FieldType.DOUBLE))

    val expected = SideTableInfo("table1", hdfsSideInfo, CreateSideTableInfo.toLogicSchema(physicsSchema), "uid", All("*", 0, TimeUnit.SECONDS), physicsSchema.toJavaList,
      Stop
    )
    val input =
      """
        |create side table(
        |     uid int,
        |     address as address string,
        |     salary double,
        |     key(uid),
        |     cache = all('*',0,s)
       ) required( source = 'hdfs',
       |            keyEncoder = 'none',
       |            castError = 'stop' )
       |  optional( path = '/root/path/file',
       |             fsDefaultName  = 'hdfs://192.168.0.1',
       |             dataType = 'json',
       |             zip = 'none') as table1
        |
        """.stripMargin

    val parserResult = CreateSideTableParser(input, Map(), Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }


  //测试基本的建表逻辑
  test("good test create redis string side table: simple") {
    val redisSideInfo = RedisSideInfo(RedisDataType.String, "192.168.0.1:2181", "******", "xxxxxx", 2, new StringFieldEncoder, new StringFieldDecoder)
    val physicsSchema = Seq(SourceField.constructSimpleCreateField("address", None, None, FieldType.STRING))

    val expected = SideTableInfo("table1", redisSideInfo, CreateSideTableInfo.toLogicSchema(physicsSchema), "uid", Nothing, physicsSchema.toJavaList, Stop)
    val input =
      """
        |create side table(
        |     address string,
        |     key(uid),
        |     cache = none
        |) required( source = 'redis',
        |            keyEncoder = 'string',
        |            castError = 'stop'
        |           )
        |   optional( url = '192.168.0.1:2181',
        |            password = '******',
        |            username = 'xxxxxx',
        |            database  = '2',
        |            dataType = 'string',
        |            decoder = 'string'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateSideTableParser(input, Map(), Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }

  //测试基本的建表逻辑
  test("good test create redis hash side table: simple") {

    val redisSideInfo = RedisSideInfo(RedisDataType.Hash, "192.168.0.1:2181", "******", "xxxxxx", 2, new StringFieldEncoder, new StringFieldDecoder)
    val physicsSchema = Seq(SourceField.constructSimpleCreateField("age", None, None, FieldType.INT),
      SourceField.constructSimpleCreateField("addr", Some("address"), Some(""), FieldType.STRING))

    val expected = SideTableInfo("table1", redisSideInfo, CreateSideTableInfo.toLogicSchema(physicsSchema), "uid", Nothing, physicsSchema.toJavaList, Stop)
    val input =
      """
        |create side table(
        |     age int,
        |     addr as address string '',
        |     key(uid),
        |     cache = none
        |) required( source = 'redis',
        |            keyEncoder = 'string',
        |            castError = 'stop'
        |           )
        |   optional( url = '192.168.0.1:2181',
        |            password = '******',
        |            username = 'xxxxxx',
        |            database  = '2',
        |            dataType = 'hash',
        |            decoder = 'string'
        |           ) as table1
        |
        """.stripMargin

    val parserResult = CreateSideTableParser(input, Map(), Map())
    assert(parserResult.isRight)
    val actual = parserResult.right.get
    assert(expected == actual)
  }
}
