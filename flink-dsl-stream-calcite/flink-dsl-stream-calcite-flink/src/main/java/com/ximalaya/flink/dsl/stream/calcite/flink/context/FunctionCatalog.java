package com.ximalaya.flink.dsl.stream.calcite.flink.context;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.*;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.functions.ScalarFunction;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/5/31
 **/

public class FunctionCatalog {

    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> nonParametricFunctions;
    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> unaryFunctions;
    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> binaryFunctions;
    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> ternaryFunctions;
    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> listFunctions;
    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> mapFunctions;
    private final Map<Pair<String, SqlKind>, Class<? extends Evaluation>> listWithOneFunctions;

    private final Map<String, ScalarFunction> dynamicFunctions;

    private FunctionCatalog(Map<Pair<String, SqlKind>, Class<? extends Evaluation>> nonParametricFunctions,
                           Map<Pair<String, SqlKind>, Class<? extends Evaluation>> unaryFunctions,
                           Map<Pair<String, SqlKind>, Class<? extends Evaluation>> binaryFunctions,
                           Map<Pair<String, SqlKind>, Class<? extends Evaluation>> ternaryFunctions,
                           Map<Pair<String, SqlKind>, Class<? extends Evaluation>> listFunctions,
                           Map<Pair<String, SqlKind>, Class<? extends Evaluation>> mapFunctions,
                           Map<Pair<String, SqlKind>, Class<? extends Evaluation>> listWithOneFunctions,
                           Map<String, ScalarFunction> dynamicFunctions) {
        this.nonParametricFunctions = nonParametricFunctions;
        this.unaryFunctions = unaryFunctions;
        this.binaryFunctions = binaryFunctions;
        this.ternaryFunctions = ternaryFunctions;
        this.listFunctions = listFunctions;
        this.mapFunctions = mapFunctions;
        this.listWithOneFunctions = listWithOneFunctions;
        this.dynamicFunctions = dynamicFunctions;
    }

    public Evaluation createFunction(String name,SqlKind kind,Object...objects) throws Exception {
        if (nonParametricFunctions.containsKey(Pair.of(name, kind))) {
            return createFunction(name, kind);
        }
        if (unaryFunctions.containsKey(Pair.of(name, kind))) {
            return createFunction(name, kind, (Evaluation) objects[0]);
        }
        if (binaryFunctions.containsKey(Pair.of(name, kind))) {
            return createFunction(name, kind, (Evaluation) objects[0], (Evaluation) objects[1]);
        }
        if (ternaryFunctions.containsKey(Pair.of(name, kind))) {
            return createFunction(name, kind, (Evaluation) objects[0], (Evaluation) objects[1], (Evaluation) objects[2]);
        }
        if (listWithOneFunctions.containsKey(Pair.of(name,kind))){
            return createFunction(name,kind,(Evaluation) objects[0], Arrays.stream(Arrays.copyOfRange(objects, 1, objects.length)).
                    map(e -> (Evaluation) e).collect(Collectors.toList()));
        }
        return createFunction(name, kind, Arrays.stream(objects).map(e -> (Evaluation) e).collect(Collectors.toList()));
    }

    private Evaluation createFunction(String name, SqlKind sqlKind) throws Exception{
        Class<? extends Evaluation> clazz = nonParametricFunctions.get(Pair.of(name,sqlKind));
        return clazz.newInstance();
    }

    private Evaluation createFunction(String name,SqlKind sqlKind,Evaluation single) throws Exception{
        Class<? extends Evaluation> clazz =unaryFunctions.get(Pair.of(name,sqlKind));
        return clazz.getConstructor(Evaluation.class).newInstance(single);
    }

    private Evaluation createFunction(String name,SqlKind sqlKind,Evaluation left,Evaluation right) throws Exception{
        Class<? extends Evaluation> clazz = binaryFunctions.get(Pair.of(name,sqlKind));
        return clazz.getConstructor(Evaluation.class,Evaluation.class).newInstance(left,right);
    }

    private Evaluation createFunction(String name,SqlKind sqlKind,Evaluation left,
                                      Evaluation middle,Evaluation right) throws Exception{
        Class<? extends Evaluation> clazz = ternaryFunctions.get(Pair.of(name,sqlKind));
        return clazz.getConstructor(Evaluation.class,Evaluation.class,Evaluation.class).newInstance(left,middle,right);
    }

    private Evaluation createFunction(String name, SqlKind sqlKind, List<Evaluation> list) throws Exception{
        Class<? extends Evaluation> clazz = listFunctions.get(Pair.of(name,sqlKind));
        if(clazz!=null) {
            return clazz.getConstructor(List.class).newInstance(list);
        }else{
            ScalarFunction udf = dynamicFunctions.get(name);
            return new UserDefineFunction(udf,list);
        }
    }

    private Evaluation createFunction(String name, SqlKind sqlKind,Evaluation single,List<Evaluation> list) throws Exception{
        Class<? extends Evaluation> clazz = listWithOneFunctions.get(Pair.of(name,sqlKind));
        return clazz.getConstructor(Evaluation.class,List.class).newInstance(single,list);
    }

    public Evaluation between(Evaluation left, Evaluation middle, Evaluation right,
                                                      boolean includeBound,boolean between){
        return new ComparisonFunctions.Between(left,middle,right,includeBound,between);
    }

    public Evaluation in(Evaluation left,List<Evaluation> rightList){
        return new ComparisonFunctions.In(left,rightList);
    }

    public Evaluation notIn(Evaluation left,List<Evaluation> rightList){
        return new ComparisonFunctions.NoIn(left,rightList);
    }

    public Evaluation similar(Evaluation left, Evaluation middle, Evaluation right, boolean similar){
        return new StringFunctions.Similar(left,middle,right,similar);
    }


    public Evaluation caseWhen(Map<Evaluation,Evaluation> conditions,
                               Evaluation guard){
        return new ConditionalFunctions.Case(conditions,guard);
    }

    public Evaluation cast(Evaluation single, SqlDataTypeSpec dataTypeSpec){
        return new TypeConversionFunctions.Cast(single,dataTypeSpec);
    }

    public Evaluation constant(Object constant){
        return new Constant(constant);
    }

    public Evaluation variable(String name,Class<?> type){
        Preconditions.checkNotNull(type);
        return new Variable(name,type);
    }

    public Evaluation variable(String tableName,String name,Class<?> type){
        Preconditions.checkNotNull(type);
        return new WithTableVariable(name,tableName,type);
    }

    public Evaluation mapConstruct(Map<Evaluation, Evaluation> map){
        return new ValueConstructionFunctions.MapConstruct(map);
    }

    public static class Builder{
        private Map<String, ScalarFunction> dynamicFunctions = Maps.newHashMap();
        private final Class<?>[] classes;

        public Builder(Class<?>[] classes) {
            this.classes = classes;
        }

        public Builder setDynamicFunctions(Map<String, ScalarFunction> dynamicFunctions){
            this.dynamicFunctions = dynamicFunctions;
            return this;
        }

        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> nonParametricFunctions = Maps.newHashMap();
        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> unaryFunctions = Maps.newHashMap();
        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> binaryFunctions = Maps.newHashMap();
        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> ternaryFunctions = Maps.newHashMap();
        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> listFunctions = Maps.newHashMap();
        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> mapFunctions = Maps.newHashMap();
        Map<Pair<String, SqlKind>, Class<? extends Evaluation>> listWithOneFunctions = Maps.newHashMap();

        public FunctionCatalog build() throws Exception{
            for(Class<?> clazz: classes){
                Class<?>[] functionClasses = clazz.getDeclaredClasses();
                for(Class<?> functionClass:functionClasses){
                    if(!functionClass.isAnnotationPresent(EvaluationInfo.class)){
                        continue;
                    }
                    EvaluationInfo eInfo = functionClass.getAnnotation(EvaluationInfo.class);
                    String functionName = eInfo.name();
                    SqlKind sqlKind = eInfo.kind();
                    if(sqlKind != SqlKind.SIMILAR && sqlKind != SqlKind.BETWEEN &&
                            sqlKind != SqlKind.CASE &&  sqlKind!=SqlKind.LITERAL && sqlKind!=SqlKind.IDENTIFIER){
                        if(BaseUnaryEvaluation.class.isAssignableFrom(functionClass)){
                            unaryFunctions.put(Pair.of(functionName,sqlKind),functionClass.asSubclass(Evaluation.class));
                            continue;
                        }
                        if(BaseBinaryEvaluation.class.isAssignableFrom(functionClass)){
                            binaryFunctions.put(Pair.of(functionName,sqlKind),functionClass.asSubclass(Evaluation.class));
                            continue;
                        }
                        if(BaseTernaryEvaluation.class.isAssignableFrom(functionClass)){
                            ternaryFunctions.put(Pair.of(functionName,sqlKind),functionClass.asSubclass(Evaluation.class));
                            continue;
                        }
                        if(BaseListEvaluation.class.isAssignableFrom(functionClass)){
                            listFunctions.put(Pair.of(functionName,sqlKind),functionClass.asSubclass(Evaluation.class));
                            continue;
                        }
                        if(BaseMapEvaluation.class.isAssignableFrom(functionClass)){
                            mapFunctions.put(Pair.of(functionName,sqlKind),functionClass.asSubclass(Evaluation.class));
                            continue;
                        }
                        Constructor<?>[] constructors = functionClass.getConstructors();
                        for(Constructor<?> constructor:constructors){
                            if(constructor.getParameterCount()==0){
                                nonParametricFunctions.put(Pair.of(functionName,sqlKind),
                                        functionClass.asSubclass(Evaluation.class));
                                break;
                            }
                            if(constructor.getParameterCount()==2 &&
                                    constructor.getParameterTypes()[0] == Evaluation.class &&
                                    constructor.getParameterTypes()[1] == List.class){
                                listWithOneFunctions.put(Pair.of(functionName,sqlKind),
                                        functionClass.asSubclass(Evaluation.class));
                                break;
                            }
                        }
                    }
                }
            }
            return new FunctionCatalog(nonParametricFunctions,unaryFunctions,binaryFunctions,
                    ternaryFunctions,listFunctions,mapFunctions,listWithOneFunctions,dynamicFunctions);
        }
    }

}
