package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.functions.RuntimeContext;

import static org.apache.calcite.runtime.SqlFunctions.*;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * 实现大部分Flink SQL字符串函数
 *
 * version  1.8.0
 */

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/

public class StringFunctions {

    //todo 未实现regex_replace overlay substring replace regex_extract lpad rpad
    @EvaluationInfo(name = "CHARLENGTH",
            desc = "returns the length of this string",
            kind = SqlKind.OTHER_FUNCTION)
    public static class CharLength extends BaseUnaryEvaluation{
        public CharLength(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return charLength(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "CHARACTERLENGTH",
            desc = "returns the length of this string",
            kind = SqlKind.OTHER_FUNCTION)
    public static class CharacterLength extends BaseUnaryEvaluation{
        public CharacterLength(Evaluation single) {
           super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return charLength(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "UPPER",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Upper extends BaseUnaryEvaluation{
        public Upper(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return upper(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "LOWER",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Lower extends BaseUnaryEvaluation{
        public Lower(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return lower(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "INITCAP",
            desc = "returns a new form of string with the first character of each word converted" +
                    " to uppercase and the rest characters to lowercase",
            kind = SqlKind.OTHER_FUNCTION)
    public static class InitCap extends BaseUnaryEvaluation{
        public InitCap(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return initcap(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "POSITION",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Position extends BaseBinaryEvaluation {
        public Position(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return position(left.eval(row).toString(), right.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( left.checkAndGetReturnType() == String.class &&
                    right.checkAndGetReturnType() == String.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "CONCAT",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Concat extends BaseListEvaluation {
        public Concat(List<Evaluation> list) {
            super(list);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            List<String> a = Lists.newArrayList();
            for (Evaluation evaluation : list) {
                Object b = evaluation.eval(row);
                if (b == null) {
                    return null;
                }
                a.add(b.toString());
            }
            return String.join("", a);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(list.stream().map(Evaluation::checkAndGetReturnType)
                    .allMatch(a->a == String.class));
            return String.class;
        }
    }

    @EvaluationInfo(name = "concat_ws",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ConcatWs implements Evaluation {
        private Evaluation delimiter;
        private List<Evaluation> list;

        public ConcatWs(Evaluation delimiter, List<Evaluation> list) {
            this.delimiter = delimiter;
            this.list = list;
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object delimiter = this.delimiter.eval(row);
            if (delimiter == null) {
                return null;
            }
            List<String> a = Lists.newArrayList();
            for (Evaluation evaluation : list) {
                Object b = evaluation.eval(row);
                if (b != null) {
                    a.add(b.toString());
                }
            }
            return String.join(delimiter.toString(), a);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(list.stream().map(Evaluation::checkAndGetReturnType)
                    .allMatch(a->a == String.class));
            Preconditions.checkArgument( delimiter.checkAndGetReturnType() == String.class );
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception {
            delimiter.open(context);
            for(Evaluation e:list){
                e.open(context);
            }
        }

        @Override
        public void close() throws Exception{
            delimiter.close();
            for(Evaluation e:list){
                e.close();
            }
        }
    }

    @EvaluationInfo(name = "REPEAT",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Repeat extends BaseBinaryEvaluation {
        public Repeat(Evaluation left,Evaluation right) {
            super(left,right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            String a = left.eval(row).toString();
            int b = (int) right.eval(row);
            if (b <= 1) {
                return a;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < b; i++) {
                sb.append(a);
            }
            return sb.toString();
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( left.checkAndGetReturnType() == String.class &&
                    right.checkAndGetReturnType() == Integer.TYPE);
            return String.class;
        }
    }

    @EvaluationInfo(name = "FROM_BASE64",
            desc = " string containing the resulting Base64 encoded characters",
            kind = SqlKind.OTHER_FUNCTION)
    public static class FromBase64 extends BaseUnaryEvaluation {
        private Base64.Decoder decoder;
        public FromBase64(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return new String(decoder.decode(a.toString()));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception {
            single.open(context);
            decoder = Base64.getDecoder();
        }
    }

    @EvaluationInfo(name = "TO_BASE64",
            desc = " string containing the resulting Base64 encoded characters",
            kind = SqlKind.OTHER_FUNCTION)
    public static class ToBase64 extends BaseUnaryEvaluation {
        private Base64.Encoder encoder;
        public ToBase64(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return encoder.encodeToString(a.toString().getBytes());
        }

        @Override
        public void open(RuntimeContext context) throws Exception{
            single.open(context);
            encoder = Base64.getEncoder();
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "||",
            desc = "'||' operator",
            kind = SqlKind.OTHER)
    public static class Other extends BaseBinaryEvaluation {
        public Other(Evaluation left, Evaluation right) {
           super(left,right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return left.eval(row).toString() + "" + right.eval(row).toString();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( left.checkAndGetReturnType()==String.class &&
                    right.checkAndGetReturnType()==String.class);
            return String.class;
        }

    }

    @EvaluationInfo(name = "TRIM",
            desc = "trim operator",
            kind = SqlKind.TRIM)
    public static class Trim extends BaseUnaryEvaluation {
        public Trim(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return single.eval(row).toString().trim();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "LTRIM",
            desc = "middle trim operator",
            kind = SqlKind.LTRIM)
    public static class LeftTrim extends BaseUnaryEvaluation {
        public LeftTrim(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return ltrim(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class);
            return String.class;
        }
    }
    @EvaluationInfo(name = "RTRIM",
            desc = "right trim operator",
            kind = SqlKind.LTRIM)
    public static class RightTrim extends BaseUnaryEvaluation {
        private Evaluation single;
        public RightTrim(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return rtrim(single.eval(row).toString());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( single.checkAndGetReturnType() == String.class);
            return String.class;
        }
    }

    @EvaluationInfo(name = "LIKE",
            desc = "like operator",
            kind = SqlKind.LIKE)
    public static class Like extends BaseTernaryEvaluation {
        private Evaluation left;
        private Evaluation middle;
        private Evaluation right;
        private boolean like;
        public Like(Evaluation left, Evaluation middle, Evaluation right, boolean like) {
            super(left,middle,right);
            this.like = like;
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = middle.eval(row);
            if (a == null || b == null) {
                return false;
            }
            if (right == null) {
                return like == like(a.toString(), b.toString());
            } else {
                return like == like(a.toString(), b.toString(), right.eval(row).toString());
            }
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( left.checkAndGetReturnType() == String.class &&
                    middle.checkAndGetReturnType() == String.class);
            if( right!=null){
                Preconditions.checkArgument(right.checkAndGetReturnType() == String.class);
            }
            return String.class;
        }
    }

    @EvaluationInfo(name = "SIMILAR",
            desc = "similar operator",
            kind = SqlKind.SIMILAR)
    public static class Similar extends BaseTernaryEvaluation {
        private Evaluation left;
        private Evaluation middle;
        private Evaluation right;
        private boolean similar;

        public Similar(Evaluation left, Evaluation middle, Evaluation right, boolean similar) {
            super(left,middle,right);
            this.similar = similar;
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = middle.eval(row);
            if (a == null || b == null) {
                return false;
            }
            if (right == null) {
                return similar == similar(a.toString(), b.toString());
            } else {
                return similar == similar(a.toString(), b.toString(), right.eval(row).toString());
            }
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument( left.checkAndGetReturnType() == String.class &&
                    middle.checkAndGetReturnType() == String.class);
            if( right!=null){
                Preconditions.checkArgument(right.checkAndGetReturnType() == String.class);
            }
            return String.class;
        }
    }
}
