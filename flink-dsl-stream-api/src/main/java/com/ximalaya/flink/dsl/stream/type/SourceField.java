package com.ximalaya.flink.dsl.stream.type;

import com.google.common.base.Preconditions;
import scala.Array;
import scala.Option;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/27
 **/

public class SourceField implements Serializable {

    private final Option<String[]> paths;
    private final String fieldName;
    private final Option<String> aliasName;
    private final Option<Object> defaultValue;
    private final FieldType fieldType;

    public Option<String[]> getPaths() {
        return paths;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Option<String> getAliasName() {
        return aliasName;
    }

    public Option<Object> getDefaultValue() {
        return defaultValue;
    }

    public FieldType getFieldType(){ return fieldType;}

    private SourceField(Option<String[]> paths,
                        String fieldName,
                        Option<String> aliasName,
                        Option<Object> defaultValue, FieldType fieldType) {
        this.paths = paths;
        this.fieldName = fieldName;
        this.aliasName = aliasName;
        this.defaultValue = defaultValue;
        this.fieldType = fieldType;
    }

    /**
     * construct field definition when create table
     * @param paths no or more optional paths
     * @param fieldName field name must be definited
     * @param aliasName alias name is optional
     * @param defaultValue default value is optional
     * @return create table field
     */
    public static SourceField constructCreateField(Option<String[]> paths,
                                                   String fieldName,
                                                   Option<String> aliasName,
                                                   Option<Object> defaultValue,FieldType fieldType){
        Preconditions.checkNotNull(paths);
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(aliasName);
        return new SourceField(paths,fieldName,aliasName,defaultValue,fieldType);
    }

    /**
     * construct simple field definition when create table
     * @param fieldName field name must be definited
     * @param aliasName alias name is optional
     * @param defaultValue default value is optional
     * @return
     */
    public static SourceField constructSimpleCreateField(String fieldName,
                                                         Option<String> aliasName,
                                                         Option<Object> defaultValue,FieldType fieldType){
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(aliasName);
        return new SourceField(Option.empty(),fieldName,aliasName,defaultValue,fieldType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceField that = (SourceField) o;
        return equalsPaths(that.paths) &&
                Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(aliasName, that.aliasName) &&
                equalsDefaultValue(this.defaultValue,that.defaultValue) &&
                fieldType == that.fieldType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(paths, fieldName, aliasName, defaultValue, fieldType);
    }

    public static boolean equalsDefaultValue(Option<Object> leftOption,Option<Object> rightOption){

        if(leftOption.isDefined() && rightOption.isDefined()){
            Object left = leftOption.get();
            Object right = rightOption.get();
            if(left==null && right == null){
                return true;
            }
            if(left.getClass().isArray() && right.getClass().isArray()){
                Class<?> leftCClazz =  left.getClass().getComponentType();
                Class<?> rightCClazz =  right.getClass().getComponentType();
               if(leftCClazz == Integer.TYPE && rightCClazz == Integer.TYPE){
                   return Arrays.equals((int[])left,(int[])right);
               }else if(leftCClazz == Double.TYPE && rightCClazz == Double.TYPE){
                   return Arrays.equals((double[])left,(double[])right);
               }else if(leftCClazz == Float.TYPE && rightCClazz == Float.TYPE){
                   return Arrays.equals((float[])left,(float[])right);
               }else if(leftCClazz == Long.TYPE && rightCClazz == Long.TYPE){
                   return Arrays.equals((long[])left,(long[])right);
               }
               return Arrays.equals((Object[])left,(Object[])right);
            }else{
                return Objects.equals(left,right);
            }
        }else if(leftOption.isEmpty() && rightOption.isEmpty()){
            return true;
        }else{
            return false;
        }
    }

    private boolean equalsPaths(Option<String[]> option){
        if(this.paths.isDefined() && option.isDefined()){
            return Arrays.equals(paths.get(),option.get());
        }else if(this.paths.isEmpty() && option.isEmpty()){
            return true;
        }else{
            return false;
        }
    }

    private String showDefault(){
        if(defaultValue.isDefined()){
            Object value = defaultValue.get();
            if(value == null){
                return "null";
            }
            if(value.getClass().isArray()){
                Class<?> cClazz =  value.getClass().getComponentType();
                if(cClazz == Integer.TYPE){
                    return Arrays.toString((int[])value);
                }else if(cClazz == Double.TYPE){
                    return Arrays.toString((double[])value);
                }else if(cClazz == Long.TYPE){
                    return Arrays.toString((long[])value);
                }else if(cClazz == Float.TYPE){
                    return Arrays.toString((float[])value);
                }else{
                    return Arrays.toString((Object[])value);
                }
            }else{
                return value.toString();
            }
        }else{
            return defaultValue.toString();
        }
    }

    @Override
    public String toString() {
        return "SourceField{" +
                "paths=" + (paths.isDefined()?Arrays.toString(paths.get()):paths) +
                ", fieldName='" + fieldName + '\'' +
                ", aliasName=" + aliasName +
                ", defaultValue=" + showDefault() +
                ", fieldType=" + fieldType +
                '}';
    }
}
