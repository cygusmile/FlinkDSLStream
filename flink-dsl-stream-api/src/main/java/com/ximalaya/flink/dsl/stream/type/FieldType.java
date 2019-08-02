package com.ximalaya.flink.dsl.stream.type;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/28
 **/
public enum FieldType {
    /**type about insert field**/
    INT(Integer.TYPE,"int"),
    LONG(Long.TYPE,"long"),
    BOOL(Boolean.TYPE,"bool"),
    FLOAT(Float.TYPE,"float"),
    DOUBLE(Double.TYPE,"double"),
    BYTE(Byte.TYPE,"byte"),
    STRING(String.class,"string"),
    OBJECT(Object.class,"object"),
    BYTE_ARRAY(byte[].class,"byte_array"),
    INT_ARRAY(int[].class,"int_array"),
    LONG_ARRAY(long[].class,"long_array"),
    FLOAT_ARRAY(float[].class,"float_array"),
    DOUBLE_ARRAY(double[].class,"double_array"),
    STRING_ARRAY(String[].class,"string_array"),
    OBJECT_ARRAY(Object[].class,"object_array"),
    MAP(Map.class,"map"),
    CLAZZ(Class.class,"clazz");

    private Class<?> clazz;
    private String typeName;

    public Class<?> getClazz(){
        return clazz;
    }

    public String getTypeName() { return typeName;}

    public static FieldType get(String typeName){
        for(FieldType fieldType:FieldType.values()){
            if(fieldType.typeName.equals(typeName)){
                return fieldType;
            }
        }
        throw new RuntimeException("this type does not found fieldType: "+typeName);
    }

    FieldType(Class<?> clazz,String typeName){
        this.clazz = clazz;
        this.typeName = typeName;
    }

    static Map<Class<?>,FieldType> classFieldTypeMap = Maps.newHashMap();

    static {
        for(FieldType fieldType:FieldType.values()){
            classFieldTypeMap.put(fieldType.clazz,fieldType);
        }
        classFieldTypeMap.put(Integer.class,INT);
        classFieldTypeMap.put(Long.class,LONG);
        classFieldTypeMap.put(BOOL.clazz,BOOL);
        classFieldTypeMap.put(Float.class,FLOAT);
        classFieldTypeMap.put(Double.class,DOUBLE);
        classFieldTypeMap.put(Byte.class,BYTE);
        classFieldTypeMap.put(Byte[].class,BYTE_ARRAY);
        classFieldTypeMap.put(Integer[].class,INT_ARRAY);
        classFieldTypeMap.put(Long[].class,LONG_ARRAY);
        classFieldTypeMap.put(Float[].class,FLOAT_ARRAY);
        classFieldTypeMap.put(Double[].class,DOUBLE_ARRAY);
        classFieldTypeMap.put(String[].class,STRING_ARRAY);
        classFieldTypeMap.put(Object[].class,OBJECT_ARRAY);
        classFieldTypeMap.put(Map.class,MAP);
        classFieldTypeMap.put(Class.class,CLAZZ);
    }

    public static FieldType get(Class<?> clazz){
        FieldType fieldType=classFieldTypeMap.get(clazz);
        return fieldType==null?OBJECT:fieldType;
    }
}
