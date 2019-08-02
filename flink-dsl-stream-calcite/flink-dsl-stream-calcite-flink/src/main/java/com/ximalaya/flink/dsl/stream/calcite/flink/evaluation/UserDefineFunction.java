package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.SCALAR_EVAL_NAME;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/
class Parameters implements Serializable {
    private Class<?>[] parameters;
    private Parameters(Class<?>[] parameters) {
        this.parameters = parameters;
    }
    static Parameters of(Class<?>[] parameters){
        return new Parameters(parameters);
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Parameters that = (Parameters) o;
        return Arrays.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(parameters);
    }
}

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/
public class UserDefineFunction implements Evaluation {

    private List<Evaluation> list;
    private ScalarFunction udf;
    private transient Method method;

    private Class<?>[] parameters;

    public UserDefineFunction(ScalarFunction udf, List<Evaluation> list) {
        this.udf = udf;
        this.list = list;
        this.parameters = new Class[list.size()];
    }

    @Override
    public void open(RuntimeContext context) throws Exception {
        udf.open(new FunctionContext(context));
        method = Arrays.stream(udf.getClass().getDeclaredMethods()).filter(m -> SCALAR_EVAL_NAME.equals(m.getName()) &&
                Arrays.equals(parameters, m.getParameterTypes())).findFirst().orElse(null);
        Preconditions.checkNotNull(method);
        method.setAccessible(true);
    }

    @Override
    public void close() throws Exception {
        udf.close();
    }

    @Override
    public Object eval(Map<String, Object> row) {
        try {
            return method.invoke(udf, list.stream().map(e -> e.eval(row)).toArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Evaluation eager() {
        this.list = list.stream().map(Evaluation::eager).collect(Collectors.toList());
        return this;
    }

    @Override
    public Evaluation eager(Map<String, Object> row) {
        this.list = list.stream().map(e->e.eager(row)).collect(Collectors.toList());
        return this;
    }

    @Override
    public Class<?> checkAndGetReturnType() {
        for (int i = 0; i < list.size(); i++) {
            parameters[i] = list.get(i).checkAndGetReturnType();
        }
        Map<Parameters, Class<?>> evalSignatures = Maps.newHashMap();
        Arrays.stream(udf.getClass().getDeclaredMethods()).
                filter(m -> SCALAR_EVAL_NAME.equals(m.getName())).
                forEach(m ->
                        evalSignatures.put(Parameters.of(m.getParameterTypes()), m.getReturnType()));
        Class<?> a =  evalSignatures.get(Parameters.of(parameters));
        Preconditions.checkNotNull(a);
        return a;
    }
}
