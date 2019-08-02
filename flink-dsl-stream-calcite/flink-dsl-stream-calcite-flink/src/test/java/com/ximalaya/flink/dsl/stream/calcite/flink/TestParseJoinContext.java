package com.ximalaya.flink.dsl.stream.calcite.flink;

import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSideJoinSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.CompileContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.SideJoinRuntimeContext;
import com.ximalaya.flink.dsl.stream.calcite.parser.DslStreamParserUtils;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.mockito.Mockito.*;
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/15
 **/
@RunWith(MockitoJUnitRunner.class)
public class TestParseJoinContext {

    @Mock
    private CompileContext compileContext;

    @Before
    public void before() throws Exception{

        doReturn(true).when(compileContext).isStreamTableRegister("streamTable");
        doReturn(true).when(compileContext).isStreamTableRegister("sTable");

        doReturn(true).when(compileContext).isSideTableRegister("dimensionTable");
        doReturn(true).when(compileContext).isSideTableRegister("dimensionTableOther");
        doReturn(true).when(compileContext).isSideTableRegister("side1");
        doReturn(true).when(compileContext).isSideTableRegister("side2");
        doReturn(true).when(compileContext).isSideTableRegister("side3");
    }

    @Test
    public void testJoin1() throws Exception{
//        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream a.*,b.info,size(c.xx) from streamTable a left join " +
//                "dimensionTable b join dimensionTableOther c on size(a.id) = b.id and a.id = cid where a.kk <> 'vv' as joinTable");
//        SqlJoin sqlJoin = (SqlJoin)((DslStreamSideJoinSqlSelect)sqlNode).getFrom();
//        SideJoinRuntimeContext.JoinContext joinContext= SqlCompiler.compile()
//        System.out.println(joinContext);
    }

    @Test
    public void testJoin2() throws Exception{
//        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream * from sTable a left join " +
//                "side1 b left join side2 join side3  on a.id = b.id and a.id = side2.c and side3.k <> a.name where a.kk <> 'vv' as joinTable");
//        SqlJoin sqlJoin = (SqlJoin)((DslStreamSideJoinSqlSelect)sqlNode).getFrom();
//        SideJoinRuntimeContext.JoinContext joinContext= SideJoinRuntimeContext.JoinContext.parseJoin(sqlJoin,compileContext);
//        System.out.println(joinContext);
    }

    @Test
    public void testJoin3() throws Exception{
//        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream * from sTable a left join " +
//                "side1 b left join side2 join side3  on a.id = b.id or a.id >= side2.c and side3.k <> a.name where a.kk <> 'vv' as joinTable");
//        SqlJoin sqlJoin = (SqlJoin)((DslStreamSideJoinSqlSelect)sqlNode).getFrom();
//        SideJoinRuntimeContext.JoinContext joinContext= SideJoinRuntimeContext.JoinContext.parseJoin(sqlJoin,compileContext);
//        System.out.println(joinContext);
    }
}
