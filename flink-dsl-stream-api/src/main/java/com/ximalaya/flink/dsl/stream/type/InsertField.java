package com.ximalaya.flink.dsl.stream.type;

import scala.Option;

import java.io.Serializable;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/3/28
 **/

public class InsertField implements Serializable {

    private final Option<String[]> paths;
    private final String fieldName;


    private InsertField(Option<String[]> paths, String fieldName) {
        this.paths = paths;
        this.fieldName = fieldName;
    }

    public Option<String[]> getPaths() {
        return paths;
    }

    public String getFieldName() {
        return fieldName;
    }

    public static InsertField constructInsertField(String fieldName, String...paths){
        if(paths.length==0){
            return new InsertField(Option.empty(),fieldName);
        }else{
            return new InsertField(Option.apply(paths),fieldName);
        }
    }
}
