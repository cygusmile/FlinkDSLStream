package com.ximalaya.flink.dsl.stream.api.message.decoder;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ximalaya.flink.dsl.stream.type.SourceField;
import scala.Option;

import java.util.*;
import java.util.function.Function;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/31
 **/

public class JsonMessageDecoder implements MessageDecoder<JSONObject> {

    @Override
    public JSONObject decode(byte[] message) {
        return JSONObject.parseObject(message,JSONObject.class);
    }

    private <T> Option<Object> resolve(SourceField sourceField, JSONObject message){
        if(sourceField.getPaths().isDefined()){
            String[] paths=sourceField.getPaths().get();
            JSONObject jsonObject = message;
            for(String path:paths){
                jsonObject=message.getJSONObject(path);
                if(jsonObject==null){
                    return Option.empty();
                }
            }
            return Option.apply(jsonObject.get(sourceField.getFieldName()));
        }else{
            return Option.apply(message.get(sourceField.getFieldName()));
        }
    }

    private <T,U> Option<T> resolve(Option<Object> option,Function<Object,U> stage1,Function<U,T> stage2){
        if(option.isDefined()){
            return Option.apply(stage2.apply(stage1.apply(option.get())));
        }else{
            return Option.empty();
        }
    }

    private int[] resolveIntArray(JSONArray jsonArray){
        Object[] objects=jsonArray.toArray(new Object[]{});
        int[] result = new int[objects.length];
        for(int i=0;i<objects.length;i++){
            result[i] = Integer.parseInt(objects[i].toString());
        }
        return result;
    }

    private long[] resolveLongArray(JSONArray jsonArray){
        Object[] objects=jsonArray.toArray(new Object[]{});
        long[] result = new long[objects.length];
        for(int i=0;i<objects.length;i++){
            result[i] = Long.parseLong(objects[i].toString());
        }
        return result;
    }

    private float[] resolveFloatArray(JSONArray jsonArray){
        Object[] objects=jsonArray.toArray(new Object[]{});
        float[] result = new float[objects.length];
        for(int i=0;i<objects.length;i++){
            result[i] = Float.parseFloat(objects[i].toString());
        }
        return result;
    }

    private double[] resolveDoubleArray(JSONArray jsonArray){
        Object[] objects=jsonArray.toArray(new Object[]{});
        double[] result = new double[objects.length];
        for(int i=0;i<objects.length;i++){
            result[i] = Double.parseDouble(objects[i].toString());
        }
        return result;
    }

    private String[] resolveStringArray(JSONArray jsonArray){
        Object[] objects=jsonArray.toArray(new Object[]{});
        String[] result = new String[objects.length];
        for(int i=0;i<objects.length;i++){
            result[i] = objects[i].toString();
        }
        return result;
    }


    private Map<Object,Object> resolveMap(JSONObject jsonObject){
        Map<Object,Object> result = new HashMap<>();
        jsonObject.keySet().forEach(key->{
            result.put(key,jsonObject.get(key));
        });
        return result;
    }

    @Override
    public Option<Integer> resolveInt(SourceField sourceField,
                                 JSONObject message) {
        return resolve(resolve(sourceField,message),Object::toString,Integer::parseInt);
    }

    @Override
    public Option<String> resolveString(SourceField sourceField,
                                          JSONObject message) {
        return resolve(resolve(sourceField,message),Object::toString,Object::toString);
    }

    @Override
    public Option<Boolean> resolveBoolean(SourceField sourceField,
                                            JSONObject message) {
        return resolve(resolve(sourceField,message),Object::toString,Boolean::parseBoolean);
    }

    @Override
    public Option<Long> resolveLong(SourceField sourceField,
                                    JSONObject message) {
        return resolve(resolve(sourceField,message),Object::toString,Long::parseLong);
    }

    @Override
    public Option<Float> resolveFloat(SourceField sourceField,
                                        JSONObject message) {
        return resolve(resolve(sourceField,message),Object::toString,Float::parseFloat);
    }

    @Override
    public Option<Double> resolveDouble(SourceField sourceField,
                                        JSONObject message) {
        return resolve(resolve(sourceField,message),Object::toString,Double::parseDouble);
    }

    @Override
    public Option<Object> resolveObject(SourceField sourceField,
                                          JSONObject message) {
        return resolve(sourceField,message);
    }

    @Override
    public Option<int[]> resolveIntArray(SourceField sourceField,
                                           JSONObject message) {
        return resolve(resolve(sourceField,message),JSONArray.class::cast,this::resolveIntArray);
    }

    @Override
    public Option<long[]> resolveLongArray(SourceField sourceField,
                                             JSONObject message) {
        return resolve(resolve(sourceField,message),JSONArray.class::cast
        ,this::resolveLongArray);
    }

    @Override
    public Option<double[]> resolveDoubleArray(SourceField sourceField,
                                                 JSONObject message) {
        return resolve(resolve(sourceField,message)
                ,JSONArray.class::cast,
               this::resolveDoubleArray);
    }

    @Override
    public Option<float[]> resolveFloatArray(SourceField sourceField,
                                               JSONObject message) {
        return resolve(resolve(sourceField,message)
                ,JSONArray.class::cast
               ,this::resolveFloatArray);
    }

    @Override
    public Option<Object[]> resolveObjectArray(SourceField sourceField,
                                                 JSONObject message) {
        return resolve(resolve(sourceField,message)
                ,JSONArray.class::cast,
                jsonArray-> jsonArray.toArray(new Object[]{}));
    }

    @Override
    public Option<Map<Object, Object>> resolveMap(SourceField sourceField,
                                                    JSONObject message) {
        return resolve(resolve(sourceField,message),JSONObject.class::cast,this::resolveMap);
    }

    @Override
    public Option<String[]> resolveStringArray(SourceField sourceField,JSONObject message) {
        return resolve(resolve(sourceField,message),JSONArray.class::cast,this::resolveStringArray);
    }

    @Override
    public String toString() {
        return "JsonMessageDecoder";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JsonMessageDecoder;
    }
}
