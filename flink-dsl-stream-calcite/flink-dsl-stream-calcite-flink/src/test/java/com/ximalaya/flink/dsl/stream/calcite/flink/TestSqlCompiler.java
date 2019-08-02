package com.ximalaya.flink.dsl.stream.calcite.flink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ximalaya.flink.dsl.stream.api.field.encoder.FieldEncoder;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.*;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.*;
import com.ximalaya.flink.dsl.stream.calcite.flink.process.explode.QueryProcessFunctionFactory;
import com.ximalaya.flink.dsl.stream.calcite.flink.process.side.SideJoinProcessFunctionFactory;
import com.ximalaya.flink.dsl.stream.side.*;
import com.ximalaya.flink.dsl.stream.type.SourceField;
import com.ximalaya.flink.dsl.stream.udf.ArrayFunctions;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import scala.Function0;
import scala.Tuple2;
import scala.concurrent.Future;
import scala.concurrent.Promise;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;


/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/8
 **/
@RunWith(MockitoJUnitRunner.class)
public class TestSqlCompiler {

    @Mock
    private CompileContext compileContext;

    private RuntimeContext runtimeContext;

    private Map<String,Object> row = Maps.newHashMap();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void before() throws Exception{
        Map<String, ScalarFunction> dynamicUDFs = Maps.newHashMap();
        dynamicUDFs.put("testUDF",new ArrayFunctions.ArrayJoin());

        row.put("userId",200L);
        row.put("name","rookie");
        row.put("interest",new String[]{"play","read","talk"});
        row.put("salary",123.3);
        row.put("scores",new int[]{1,2,3,1});


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

        Map<String,Class<?>> schema = Maps.newLinkedHashMap();

        schema.put("userId",Long.TYPE);
        schema.put("name",String.class);
        schema.put("interest",String[].class);
        schema.put("salary",Double.TYPE);
        schema.put("scores",int[].class);

        doReturn(functionCatalog).when(compileContext).getFunctionCatalog();
        doReturn(schema).when(compileContext).queryStreamSchema("testTable");
        runtimeContext = new RuntimeUDFContext(new TaskInfo("task",10,1,10,10),
                null,null,Maps.newHashMap(),Maps.newHashMap(),null);


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

        PhysicsSideInfo physicsSideInfo1 = new PhysicsSideInfo() {
            @Override
            public String name() {
                return "test1";
            }

            @Override
            public FieldEncoder keyEncoder() {
                return null;
            }
        };

        PhysicsSideInfo physicsSideInfo2 = new PhysicsSideInfo() {
            @Override
            public String name() {
                return "test2";
            }

            @Override
            public FieldEncoder keyEncoder() {
                return null;
            }
        };

        Map<String, Class<? extends AsyncSideClient>> map = Maps.newHashMap();

        map.put("test1",TestAsyncSideClient1.class);
        map.put("test2",TestAsyncSideClient2.class);

        SideCatalog sideCatalog = new SideCatalog(map,Maps.newHashMap());

        doReturn(sideCatalog).when(compileContext).getSideCatalog();

        SideTableInfo sideContextB = SideTableInfo.apply("jobInfo",physicsSideInfo1,sideSchemaB,
                "id",new Lru("*",3,2, TimeUnit.MILLISECONDS),null, null);
        doReturn(Optional.of(sideContextB)).when(compileContext).querySideSchema("jobInfo");

        Map<String, Class<?>> sideSchemaC = Maps.newHashMap();
        sideSchemaC.put("id", Long.TYPE);
        sideSchemaC.put("name", String.class);

        SideTableInfo sideContextC = SideTableInfo.apply("salaryInfo",physicsSideInfo2,sideSchemaC,
                "id",new Lru("*",3,2, TimeUnit.MILLISECONDS),null,null);
        doReturn(Optional.of(sideContextC)).when(compileContext).querySideSchema("salaryInfo");

        when(compileContext.queryExistsFieldSideTables(anyString(), anySet())).thenAnswer(new Answer<Set<String>>() {
            @Override
            public Set<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                String fieldName = (String) invocationOnMock.getArguments()[0];
                Set<String> tables = Sets.newHashSet();
                if (sideSchemaB.containsKey(fieldName)) {
                    tables.add("jobInfo");
                }
                if (sideSchemaC.containsKey(fieldName)) {
                    tables.add("salaryInfo");
                }
                return tables;
            }
        });
        doReturn(true).when(compileContext).isStreamTableRegister("userInfo");
        doReturn(true).when(compileContext).isSideTableRegister("jobInfo");
        doReturn(true).when(compileContext).isSideTableRegister("salaryInfo");

    }

    @Test
    public void testSqlCompiler1() throws Exception{
        SqlRuntimeContext sqlRuntimeContext = SqlCompiler.compile("select explode(interest) from testTable as newMyTable",compileContext);
        Preconditions.checkNotNull(sqlRuntimeContext);
        ProcessFunction<Row, Row> processFunction =  QueryProcessFunctionFactory.createQueryProcessFunction((QueryRuntimeContext)sqlRuntimeContext);
        processFunction.setRuntimeContext(runtimeContext);
        processFunction.open(new Configuration());
        List<Row> rows = Lists.newArrayList();
        Collector<Row> out = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                rows.add(record);
            }
            @Override
            public void close() {
                //do nothing
            }
        };
        processFunction.processElement(Row.of(300L,"martin",new String[]{"talk","read","play"},2211.122,new int[]{2,1,2,3}),null,out);
        processFunction.processElement(Row.of(300L,"martin",new String[]{"run","eat","sleep"},30.2,new int[]{2,1,2,3}),null,out);
        assert rows.get(0).equals(Row.of("talk"));
        assert rows.get(1).equals(Row.of("read"));
        assert rows.get(2).equals(Row.of("play"));
        assert rows.get(3).equals(Row.of("run"));
        assert rows.get(4).equals(Row.of("eat"));
        assert rows.get(5).equals(Row.of("sleep"));
    }

    @Test
    public void testSqlCompiler2() throws Exception{
        SqlRuntimeContext sqlRuntimeContext = SqlCompiler.compile("select explode(interest) from testTable where max(min(userId,20),cast(max(salary,20.2) as bigint)) >300 as newMyTable",compileContext);
        Preconditions.checkNotNull(sqlRuntimeContext);
        ProcessFunction<Row, Row> processFunction =  QueryProcessFunctionFactory.createQueryProcessFunction((QueryRuntimeContext)sqlRuntimeContext);
        processFunction.setRuntimeContext(runtimeContext);
        processFunction.open(new Configuration());
        List<Row> rows = Lists.newArrayList();
        Collector<Row> out = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                rows.add(record);
            }
            @Override
            public void close() {
                //do nothing
            }
        };
        processFunction.processElement(Row.of(300L,"martin",new String[]{"talk","read","play"},2211.122,new int[]{2,1,2,3}),null,out);
        processFunction.processElement(Row.of(300L,"martin",new String[]{"talk","read","play"},30.2,new int[]{2,1,2,3}),null,out);
        assert rows.get(0).equals(Row.of("talk"));
        assert rows.get(1).equals(Row.of("read"));
        assert rows.get(2).equals(Row.of("play"));
    }

    @Test
    public void testSqlCompile3() throws Exception{
        SqlRuntimeContext sqlRuntimeContext = SqlCompiler.compile("select userId,score,salary from testTable lateral view explode(scores) explodeTable as score as newMyTable",compileContext);
        Preconditions.checkNotNull(sqlRuntimeContext);
        ProcessFunction<Row, Row> processFunction =  QueryProcessFunctionFactory.createQueryProcessFunction((QueryRuntimeContext)sqlRuntimeContext);
        processFunction.setRuntimeContext(runtimeContext);
        processFunction.open(new Configuration());
        List<Row> rows = Lists.newArrayList();
        Collector<Row> out = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                rows.add(record);
            }
            @Override
            public void close() {
                //do nothing
            }
        };
        processFunction.processElement(Row.of(300L,"martin",new String[]{"talk","read","play"},2211.122,new int[]{2,1,2,3}),null,out);
        processFunction.processElement(Row.of(400L,"martin",new String[]{"talk","read","play"},30.2,new int[]{10,12,21,32}),null,out);

        assert rows.get(0).equals(Row.of(300L,2,2211.122));
        assert rows.get(1).equals(Row.of(300L,1,2211.122));
        assert rows.get(2).equals(Row.of(300L,2,2211.122));
        assert rows.get(3).equals(Row.of(300L,3,2211.122));
        assert rows.get(4).equals(Row.of(400L,10,30.2));
        assert rows.get(5).equals(Row.of(400L,12,30.2));
        assert rows.get(6).equals(Row.of(400L,21,30.2));
        assert rows.get(7).equals(Row.of(400L,32,30.2));
    }

    @Test
    public void testSqlCompile4() throws Exception{
        SqlRuntimeContext sqlRuntimeContext = SqlCompiler.compile("select userId,score,salary from testTable lateral view explode(scores) explodeTable as score where max(min(userId,20),cast(max(salary,20.2) as bigint)) >300 as newMyTable",compileContext);
        Preconditions.checkNotNull(sqlRuntimeContext);
        ProcessFunction<Row, Row> processFunction =  QueryProcessFunctionFactory.createQueryProcessFunction((QueryRuntimeContext)sqlRuntimeContext);
        processFunction.setRuntimeContext(runtimeContext);
        processFunction.open(new Configuration());
        List<Row> rows = Lists.newArrayList();
        Collector<Row> out = new Collector<Row>() {
            @Override
            public void collect(Row record) {
                rows.add(record);
            }
            @Override
            public void close() {
                //do nothing
            }
        };
        processFunction.processElement(Row.of(300L,"martin",new String[]{"talk","read","play"},2211.122,new int[]{2,1,2,3}),null,out);
        processFunction.processElement(Row.of(400L,"martin",new String[]{"talk","read","play"},30.2,new int[]{10,12,21,32}),null,out);

        assert rows.get(0).equals(Row.of(300L,2,2211.122));
        assert rows.get(1).equals(Row.of(300L,1,2211.122));
        assert rows.get(2).equals(Row.of(300L,2,2211.122));
        assert rows.get(3).equals(Row.of(300L,3,2211.122));
    }

    @Test
    public void testSqlCompile5() throws Exception{
        SqlRuntimeContext sqlRuntimeContext = SqlCompiler.compile("select side stream a.uid,b.salary,salaryInfo.name from userInfo a join jobInfo b join salaryInfo on a.uid = b.id and a.uid = salaryInfo.id " +
                "where a.uid <> 2222 and a.name <> b.name or ( salaryInfo.name = 'cname'  and address = 'shanghai') as xxx",compileContext);

        RichAsyncFunction<Row,Row> processFunction = (RichAsyncFunction<Row,Row>)SideJoinProcessFunctionFactory.createSideProcessFunction((SideJoinRuntimeContext) sqlRuntimeContext,compileContext);
        ResultFuture<Row> rowResultFuture = new ResultFuture<Row>() {
            @Override
            public void complete(Collection<Row> result) {
                result.forEach(row->{
                    System.out.println(row);
                });
            }

            @Override
            public void completeExceptionally(Throwable error) {

            }
        };

        processFunction.setRuntimeContext(runtimeContext);
        processFunction.open(null);

        processFunction.asyncInvoke(Row.of(300L,"martin",12,"play","xxxx"),rowResultFuture);

        Thread.sleep(1000);
    }


}
