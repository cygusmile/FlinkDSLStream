package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/3
 **/

class EvaluationUtils {

    static final String SCALAR_EVAL_NAME = "eval";

    static boolean isInt(Object a, Object b){
        return a.getClass() == Integer.class && b.getClass() == Integer.class;
    }

    static boolean isInt(Object a){
        return a.getClass() == Integer.class;
    }

    static boolean isLong(Object a){
        return a.getClass() == Long.class;
    }

    static boolean isLong(Object a, Object b){
        return a.getClass() == Long.class && b.getClass() == Long.class;
    }

    static boolean isDouble(Object a){
        return a.getClass() == Double.class;
    }

    static boolean isDouble(Object a, Object b){
        return a.getClass() == Double.class && b.getClass() == Double.class;
    }

    static boolean isFloat(Object a){ return a.getClass() == Float.class; }

    static boolean isFloat(Object a, Object b){
        return a.getClass() == Float.class && b.getClass() == Float.class;
    }

    static boolean isArray(Object a){ return a.getClass().isArray(); }

    static boolean isIntArray(Object a){ return isArray(a) && a.getClass().getComponentType() ==Integer.TYPE;}

    static boolean isLongArray(Object a){ return isArray(a) && a.getClass().getComponentType() ==Long.TYPE;}

    static boolean isFloatArray(Object a){ return isArray(a) && a.getClass().getComponentType() == Float.TYPE;}

    static boolean isDoubleArray(Object a){ return isArray(a) && a.getClass().getComponentType() == Double.TYPE;}

    static boolean isArrayArray(Object a){ return isArray(a)
            && isArray(a.getClass().getComponentType());}

    static boolean isIntArrayArray(Object a){ return isArray(a) &&
            isArray(a.getClass().getComponentType()) &&
            isInt(a.getClass().getComponentType().getComponentType());}

    static boolean isLongArrayArray(Object a){ return isArray(a) &&
            isArray(a.getClass().getComponentType()) &&
            isLong(a.getClass().getComponentType().getComponentType());}

    static boolean isFloatArrayArray(Object a){ return isArray(a) &&
            isArray(a.getClass().getComponentType()) &&
            isFloat(a.getClass().getComponentType().getComponentType());}

    static boolean isDoubleArrayArray(Object a){ return isArray(a) &&
            isArray(a.getClass().getComponentType()) &&
            isDouble(a.getClass().getComponentType().getComponentType());}

    static Class<?> getArithmeticReturnType(Class<?> left,Class<?> right) {
        Preconditions.checkArgument(
                left == Integer.class ||
                        left == Double.class ||
                        left == Long.class ||
                        left ==Float.class);
        Preconditions.checkArgument(
                right == Integer.class ||
                        right == Double.class ||
                        right == Long.class ||
                        right ==Float.class);
        if (left.isAssignableFrom(Number.class) && right.isAssignableFrom(Number.class)) {
            if (left == Integer.class && right == Long.class ||
                    right == Integer.class && left == Long.class) {
                return Long.class;
            }
            if (left == Integer.class && right == Integer.class) {
                return Integer.class;
            }
            if (left == Float.class && right != Double.class
                    || right == Float.class && left != Double.class) {
                return Float.class;
            }
            return Double.class;
        }
        return null;
    }

    private static Class<?> getCommonSupperClass(Class<?> one,Class<?> another){
        if(one.isAssignableFrom(another)){
            return another;
        }else if(another.isAssignableFrom(one)){
            return one;
        }else{
            return Object.class;
        }
    }

    static Class<?> getCommonSupperClass(Class<?>[] paramsTypes){
        if(paramsTypes.length == 0){
            return null;
        }
        List<Class<?>> classes = Arrays.stream(paramsTypes).distinct().collect(Collectors.toList());
        if(classes.size()==1) {
            return classes.get(0);
        }
        Class<?> currentClass = classes.get(0);
        for(int i=1;i<classes.size();i++){
            Class<?> nextClass = classes.get(i);
            currentClass = getCommonSupperClass(currentClass,nextClass);
        }
        return currentClass;
    }

}
