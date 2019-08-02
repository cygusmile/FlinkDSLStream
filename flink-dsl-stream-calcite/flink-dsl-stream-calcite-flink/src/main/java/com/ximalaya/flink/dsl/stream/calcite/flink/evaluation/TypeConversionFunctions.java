package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.ximalaya.flink.dsl.stream.type.FieldType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;

import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.*;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/

public class TypeConversionFunctions {

    @EvaluationInfo(name = "CAST",
            desc = "cast operator",
            kind = SqlKind.CAST)
    public static class Cast extends BaseUnaryEvaluation{
        private FieldType fieldType;

        public Cast(Evaluation single, SqlDataTypeSpec dataTypeSpec){
            super(single);
            String typeName = dataTypeSpec.getTypeName().names.get(0);
            switch (typeName){
                case "INTEGER":this.fieldType = FieldType.INT;break;
                case "BOOLEAN":this.fieldType = FieldType.BOOL;break;
                case "TINYINT":this.fieldType = FieldType.BYTE;break;
                case "BIGINT":this.fieldType = FieldType.LONG;break;
                case "FLOAT":this.fieldType = FieldType.FLOAT;break;
                case "DOUBLE":this.fieldType = FieldType.DOUBLE;break;
                case "MAP":this.fieldType = FieldType.MAP;break;
                case "VARCHAR":this.fieldType = FieldType.STRING;break;
                default:this.fieldType = FieldType.OBJECT_ARRAY;break;
            }
        }

        private static <T extends Number> Object cast(T a,FieldType fieldType){
            switch (fieldType) {
                case INT:
                    return a.intValue();
                case FLOAT:
                    return a.floatValue();
                case LONG:
                    return a.longValue();
                case DOUBLE:
                    return a.doubleValue();
                default:
                    return null;
            }
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if(a==null){
                return null;
            }
            if(fieldType==FieldType.STRING){
                return a.toString();
            }
            if(fieldType==FieldType.BOOL){
                return Boolean.parseBoolean(a.toString());
            }
            if(isDouble(a)) {
                return cast((Double)a,fieldType);
            }
            if(isLong(a)) {
                return cast((Long)a,fieldType);
            }
            if(isFloat(a)) {
                return cast((Float)a,fieldType);
            }
            if(isInt(a)){
                return cast((Integer)a,fieldType);
            }
            return cast((Double)a,fieldType);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            single.checkAndGetReturnType();
            return fieldType.getClazz();
        }
    }
}
