package com.ximalaya.flink.dsl.stream.calcite.flink;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSideJoinSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.CompileContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.FunctionCatalog;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.SideJoinRuntimeContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.*;
import com.ximalaya.flink.dsl.stream.calcite.parser.DslStreamParserUtils;
import com.ximalaya.flink.dsl.stream.udf.ArrayFunctions;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.*;
/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/17
 **/
@RunWith(MockitoJUnitRunner.class)
public class TestParseJoinWhere {

    @Mock
    private CompileContext compileContext;

//    @Mock
//    private SideJoinRuntimeContext.JoinContext joinContext;


    private Map<String,Object> streamRow = Maps.newHashMap();
    {
        streamRow.put("userInfo_uid",1111L);
        streamRow.put("userInfo_name","liu");
        streamRow.put("userInfo_age",22);
        streamRow.put("userInfo_job","worker");
        streamRow.put("userInfo_address","shanghai Lu");
    }


    @Before
    public void before() throws Exception {
//        doReturn("userInfo").when(joinContext).getStreamTable();
//        doReturn(Sets.newHashSet( "jobInfo", "salaryInfo")).when(joinContext).getSideTables();
//
//
//        Map<String, String> tableRealNameMap = Maps.newHashMap();
//        tableRealNameMap.put("a", "userInfo");
//        tableRealNameMap.put("b", "jobInfo");
//        tableRealNameMap.put("salaryInfo", "salaryInfo");
//        doReturn(tableRealNameMap).when(joinContext).getTableRealNameMap();
//
//        Map<String, Class<?>> streamSchema = Maps.newHashMap();
//        streamSchema.put("uid", Long.TYPE);
//        streamSchema.put("name", String.class);
//        streamSchema.put("age", Integer.TYPE);
//        streamSchema.put("job", String.class);
//        streamSchema.put("address",String.class);
//        doReturn(streamSchema).when(compileContext).queryStreamSchema("userInfo");
//
//        Map<String, Class<?>> sideSchemaB = Maps.newHashMap();
//        sideSchemaB.put("id", Long.TYPE);
//        sideSchemaB.put("name", String.class);
//        sideSchemaB.put("salary", Double.TYPE);
//
//
//        doReturn(Optional.empty()).when(compileContext).querySideSchema("userInfo");
//
//        SideContext sideContextB = new SideContext(sideSchemaB,"id");
//        doReturn(Optional.of(sideContextB)).when(compileContext).querySideSchema("jobInfo");
//
//        Map<String, Class<?>> sideSchemaC = Maps.newHashMap();
//        sideSchemaC.put("id", Long.TYPE);
//        sideSchemaC.put("name", String.class);
//
//        SideContext sideContextC = new SideContext(sideSchemaC,"id");
//        doReturn(Optional.of(sideContextC)).when(compileContext).querySideSchema("salaryInfo");
//
//
//        Map<String, ScalarFunction> dynamicUDFs = Maps.newHashMap();
//        dynamicUDFs.put("testUDF", new ArrayFunctions.ArrayJoin());
//        FunctionCatalog functionCatalog = new FunctionCatalog.Builder(new Class<?>[]{
//                ArithmeticFunctions.class,
//                BuildInUserDefineFunction.class,
//                CollectionFunctions.class,
//                ComparisonFunctions.class,
//                ConditionalFunctions.class,
//                HashFunctions.class,
//                LogicalFunctions.class,
//                StringFunctions.class,
//                TemporalFunctions.class,
//                TypeConversionFunctions.class,
//                ValueConstructionFunctions.class
//        }).setDynamicFunctions(dynamicUDFs).build();
//
//        doReturn(functionCatalog).when(compileContext).getFunctionCatalog();
//
//        when(compileContext.queryExistsFieldSideTables(anyString(), anySet())).thenAnswer(new Answer<Set<String>>() {
//            @Override
//            public Set<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
//                String fieldName = (String) invocationOnMock.getArguments()[0];
//                Set<String> tables = Sets.newHashSet();
//                if (sideSchemaB.containsKey(fieldName)) {
//                    tables.add("jobInfo");
//                }
//                if (sideSchemaC.containsKey(fieldName)) {
//                    tables.add("salaryInfo");
//                }
//                return tables;
//            }
//        });
    }

    @Test
    public void testJoinWhere1() throws Exception {
//        SqlNode sqlNode = DslStreamParserUtils.parse("select stream a.uid,b.job,c.salary from userInfo a join jobInfo" +
//                " b join salaryInfo on a.id = b.id and a.id = salaryInfo.id where a.uid <> 2222 and a.name <> b.name or ( salaryInfo.name = 'cname'  and address = 'shanghai') as xxx");
//        SqlNode where = ((DslStreamSideJoinSqlSelect) sqlNode).getWhere();
//        Evaluation evaluation = SqlCompiler.genCode(where, "userInfo", joinContext, compileContext);
//        evaluation.checkAndGetReturnType();
//        evaluation = evaluation.eager();
//        evaluation = evaluation.eager(streamRow);
//
//        Map<String,Object> sideBRow = Maps.newHashMap();
//        sideBRow.put("jobInfo_name","wang");
//        assert (boolean)evaluation.eval(sideBRow);
//
//        sideBRow.put("jobInfo_name","liu");
//        assert !(boolean)evaluation.eval(sideBRow);
    }
}