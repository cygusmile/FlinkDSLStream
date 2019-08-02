package com.ximalaya.flink.dsl.stream.calcite.flink;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.CompileContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.FunctionCatalog;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.SideJoinRuntimeContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.*;
import com.ximalaya.flink.dsl.stream.calcite.parser.DslStreamParserUtils;
import com.ximalaya.flink.dsl.stream.side.SideTableInfo;
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

import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/18
 **/
@RunWith(MockitoJUnitRunner.class)
public class TestParseSideJoin {

    @Mock
    private CompileContext compileContext;

    @Before
    public void before() throws Exception{
        Map<String, Class<?>> streamSchema = Maps.newHashMap();
        streamSchema.put("uid", Long.TYPE);
        streamSchema.put("name", String.class);
        streamSchema.put("age", Integer.TYPE);
        streamSchema.put("job", String.class);
        streamSchema.put("address",String.class);
        doReturn(streamSchema).when(compileContext).queryStreamSchema("userInfo");

        Map<String, Class<?>> sideSchemaB = Maps.newHashMap();
        sideSchemaB.put("id", Long.TYPE);
        sideSchemaB.put("name", String.class);
        sideSchemaB.put("salary", Double.TYPE);


        doReturn(Optional.empty()).when(compileContext).querySideSchema("userInfo");

        SideTableInfo sideContextB = SideTableInfo.apply("jobInfo",null,sideSchemaB,"id",null,null,null);
        doReturn(Optional.of(sideContextB)).when(compileContext).querySideSchema("jobInfo");

        Map<String, Class<?>> sideSchemaC = Maps.newHashMap();
        sideSchemaC.put("id", Long.TYPE);
        sideSchemaC.put("name", String.class);

        SideTableInfo sideContextC = SideTableInfo.apply("salaryInfo",null,sideSchemaC,"id",null,null,null);
        doReturn(Optional.of(sideContextC)).when(compileContext).querySideSchema("salaryInfo");

        Map<String, ScalarFunction> dynamicUDFs = Maps.newHashMap();
        dynamicUDFs.put("testUDF", new ArrayFunctions.ArrayJoin());
        FunctionCatalog functionCatalog = new FunctionCatalog.Builder(new Class<?>[]{
                ArithmeticFunctions.class,
                BuildInUserDefineFunction.class,
                CollectionFunctions.class,
                ComparisonFunctions.class,
                ConditionalFunctions.class,
                HashFunctions.class,
                LogicalFunctions.class,
                StringFunctions.class,
                TemporalFunctions.class,
                TypeConversionFunctions.class,
                ValueConstructionFunctions.class
        }).setDynamicFunctions(dynamicUDFs).build();

        doReturn(functionCatalog).when(compileContext).getFunctionCatalog();

        when(compileContext.queryExistsFieldSideTables(anyString(), anySet())).thenAnswer(new Answer<Set<String>>() {
            @Override
            public Set<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                String fieldName = (String) invocationOnMock.getArguments()[0];
                Set<String> tables = Sets.newHashSet();
                if (sideSchemaC.containsKey(fieldName)) {
                    tables.add("salaryInfo");
                }
                if (sideSchemaB.containsKey(fieldName)) {
                    tables.add("jobInfo");
                }
                return tables;
            }
        });
        doReturn(true).when(compileContext).isStreamTableRegister("userInfo");
        doReturn(true).when(compileContext).isSideTableRegister("jobInfo");
        doReturn(true).when(compileContext).isSideTableRegister("salaryInfo");

    }

    @Test
    public void testParseSideJoin1() throws Exception{
        SqlNode sqlNode = DslStreamParserUtils.parse("select side stream a.uid,b.job,salaryInfo.name from userInfo a join jobInfo" +
                " b join salaryInfo on a.uid = b.id and a.uid = salaryInfo.id where a.uid <> 2222 and a.name <> b.name or ( salaryInfo.name = 'cname'  and address = 'shanghai') as xxx");
        SideJoinRuntimeContext sideJoinRuntimeContext = SideJoinRuntimeContext.apply(sqlNode,compileContext);
        System.out.println(sideJoinRuntimeContext);
   }

}
