package com.ximalaya.flink.dsl.stream.calcite.flink;

import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.calcite.domain.node.DslStreamSimpleExplodeSqlSelect;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.CompileContext;
import com.ximalaya.flink.dsl.stream.calcite.flink.context.FunctionCatalog;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.*;
import com.ximalaya.flink.dsl.stream.calcite.parser.DslStreamParserUtils;
import com.ximalaya.flink.dsl.stream.udf.ArrayFunctions;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/6
 **/
@RunWith(MockitoJUnitRunner.class)
public class TestCodeGenerator {

    @Mock
    private CompileContext compileContext;

    private Map<String,Object> row = Maps.newHashMap();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void before() throws Exception{

        Map<String, ScalarFunction> dynamicUDFs = Maps.newHashMap();
        dynamicUDFs.put("testUDF",new ArrayFunctions.ArrayJoin());

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

        row.put("userId",200L);
        row.put("name","rookie");
        row.put("interest",new String[]{"play","read","talk"});
        row.put("salary",123.3);
        row.put("scores",new int[]{1,2,3,1});

        Map<String,Class<?>> schema = Maps.newHashMap();

        schema.put("userId",Long.TYPE);
        schema.put("name",String.class);
        schema.put("interest",String[].class);
        schema.put("salary",Double.TYPE);
        schema.put("scores",int[].class);

        doReturn(functionCatalog).when(compileContext).getFunctionCatalog();
        doReturn(schema).when(compileContext).queryStreamSchema("testTable");
    }

    @Test
    public void testCodeGenWhere1() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where userId is not null and max(salary, 32.2) > 150 as newMyTable");
        Evaluation evaluation = SqlCompiler.
                genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        assert !(Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere2() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where userId is not null and cardinality(interest) > 1 as newMyTable");
        Evaluation evaluation = SqlCompiler.
                genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        assert (Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere3() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where concat_ws(',',name) = 'play,read,talk' as newMyTable");
        Evaluation evaluation = SqlCompiler.
                genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        assert !(Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere4() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where ArrayHead(interest) = 'play' as newMyTable");
        Evaluation evaluation = SqlCompiler.
                genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        assert (Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere5() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where testUDF(scores,',') = '1,2,3,1' as newMyTable");
        Evaluation evaluation = SqlCompiler.genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        evaluation.open(null);
        assert (Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere6() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where max(min(userId,20),cast(max(salary,20.2) as bigint)) >300 as newMyTable");
        Evaluation evaluation = SqlCompiler.genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        evaluation.open(null);
        assert !(Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere7() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) from testTable where ArraySum(array[1,2,3,4]) = 10 and cast(ArraySize(array[1,2,3,4])" +
                        " as bigint) = 4 and cast(cardinality(Map[(1,2),(3,4)]) as bigint) = 2 as newMyTable");
        Evaluation evaluation = SqlCompiler.genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        evaluation.open(null);
        assert (Boolean)evaluation.eval(row);
    }

    @Test
    public void testCodeGenWhere8() throws Exception{
        DslStreamSimpleExplodeSqlSelect sqlNode = (DslStreamSimpleExplodeSqlSelect) DslStreamParserUtils.
                parse("select explode(myCol) as $1 from testTable where userId in (100,300) as newMyTable");
        Evaluation evaluation = SqlCompiler.genCode(sqlNode.getWhere(),"testTable",null,compileContext);
        evaluation.checkAndGetReturnType();
        evaluation = evaluation.eager();
        evaluation.open(null);
        assert !(Boolean)evaluation.eval(row);
    }
}
