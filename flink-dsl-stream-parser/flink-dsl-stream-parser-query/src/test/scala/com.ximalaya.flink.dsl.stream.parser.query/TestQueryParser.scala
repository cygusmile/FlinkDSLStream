package com.ximalaya.flink.dsl.stream.parser.query

import org.scalatest.FunSuite

/**
  *
  * @author martin.dong
  *
  **/

class TestQueryParser  extends FunSuite{


  test("test query"){
    val query=
      """
         |select func1(x) as xx,func2(yyy) as yyy,zzz from table as table2
      """.stripMargin

    val result=QueryParser(query)
    assert(result.isRight)
    val (body,alias)=result.right.get
    assert(body=="select func1(x) as xx,func2(yyy) as yyy,zzz from table" && alias=="table2")
  }
}
