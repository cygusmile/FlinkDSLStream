package com.ximalaya.flink.dsl.stream.calcite.parser;

import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/29
 **/

public class TestDslStreamParser {

    @Test
    public void testExplode() throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse("select cast(kkk as varchar),case when size(xxx) = 2 then ppp when length(xxx) = 3 then ooo else zzz end,nullif(x,y),Map[(1,2),(3,4)],element(xxx) as asd,row(3,2,1),CURRENT_TIMESTAMP(),radians(1000) as yyy,mod(rrr,uuu) as ccc from myTable lateral view explode(size(myCol2,'xx')) explodeTable as explodeMyCol2 as myNewTable");
        System.out.println(sqlNode);
    }

    @Test
    public void testSimpleExplode() throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse("select explode(myCol) as pps from myTable where xxx not in (1,2,3) and xx<>'' and e() = 1 and  value1 not BETWEEN value2 AND value3 or value1 IS DISTINCT FROM value2  as newMyTable");
        System.out.println(sqlNode);
    }

    @Test
    public void testJoin1() throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream a.*,b.info,size(c.xx) from streamTable a left join dimensionTable b join dimensionTableOther c on size(a.id) = b.id and a.id = cid and c.xx <> 'xx' where a.kk <> 'vv' as joinTable");
        System.out.println(sqlNode);
    }

    @Test
    public void testJoin2() throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream a.*,b.info,size(c.xx) " +
                "from streamTable a left join dimensionTable a join dimensionTableOther c on size(a.id) = b.id and a.id = cid and c.xx <> 'xx' " +
                "where len(a.job) = 2 or b.k = a.uid and a.xx = 'xxx' and c.zz = 'xx' as joinTable");
        System.out.println(sqlNode);
    }

    @Test
    public void testJoin3() throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream a.name,a.product,b.name,b.salary from tableA a join tableB b on a.id = b.id as tableC");
        System.out.println(sqlNode);
    }
}
