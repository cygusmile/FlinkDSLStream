package com.ximalaya.flink.dsl.stream.parser.`import`

import com.ximalaya.flink.dsl.stream.`type`.FieldType
import org.scalatest.FunSuite

/**
  *
  * @author martin.dong
  *
  **/

class TestParser extends FunSuite{

  test("test comment: one"){
    val comment=
      """
        |-----xxxx xxx
      """.stripMargin

      val result=CommentParser(comment)
      assert(result.isEmpty)
  }


  test("test comment: two"){
    val comment=
      """
        |-----xxxx xxx
        |cccc
      """.stripMargin

    val result=CommentParser(comment)
    assert(result.isEmpty)
  }

  test("test include jar"){
    val include =
      """
        |include /user/ximalaya/functions.jar
      """.stripMargin

    val result = IncludeParser(include)
    assert(result.isRight && result.right.get=="/user/ximalaya/functions.jar")
  }

  test("test include path"){
    val include =
      """
        |include /user/ximalaya
      """.stripMargin

    val result = IncludeParser(include)
    assert(result.isRight && result.right.get=="/user/ximalaya")
  }

  import com.ximalaya.flink.dsl.stream.`type`.FieldType._
  test("test import udf"){
    val importUDF=
      """
        |import udf from 'com.ximalaya.SegmentWord' with(int='1',bool='true',clazz='person',object='info') as 'SegmentWord'
      """.stripMargin

    val result = ImportParser(importUDF)
    assert(result.isRight && result.right.get == ImportInfo("udf","com.ximalaya.SegmentWord",List((INT,"1"),(BOOL,"true"),(CLAZZ,"person"),(OBJECT,"info")),"SegmentWord"))
  }

  test("test import simple udf"){
    val importUDF=
      """
        |import udf from 'com.ximalaya.SegmentWord'   as 'SegmentWord'
      """.stripMargin

    val result = ImportParser(importUDF)
    assert(result.isRight && result.right.get == ImportInfo("udf","com.ximalaya.SegmentWord",List(),"SegmentWord"))
  }

  test("test import decoder"){
    val importDecoder=
      """
        |import decoder from 'com.ximalaya.SimpleDecoder'   as 'SimpleDecoder'
      """.stripMargin

    val result = ImportParser(importDecoder)
    assert(result.isRight && result.right.get == ImportInfo("decoder","com.ximalaya.SimpleDecoder",List(),"SimpleDecoder"))
  }

}
