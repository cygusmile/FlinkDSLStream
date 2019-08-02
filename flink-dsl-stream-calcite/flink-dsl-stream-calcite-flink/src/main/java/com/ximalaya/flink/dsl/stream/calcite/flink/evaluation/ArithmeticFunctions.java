package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;
import java.util.UUID;

import static org.apache.calcite.runtime.SqlFunctions.*;
import static org.apache.calcite.runtime.SqlFunctions.toBigDecimal;
import static com.ximalaya.flink.dsl.stream.calcite.flink.evaluation.EvaluationUtils.*;
/**
 * 实现大部分Flink SQL算术函数
 *
 * version: 1.8.0
 */

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/
public class ArithmeticFunctions {
    //todo 未实现log2 log(num1,num2) truncate方法
    @EvaluationInfo(name = "ABS",
            desc = "returns the smallest (closest to negative infinity) double value " +
                    "that is greater than or equal to the argument " +
                    "and is equal to a mathematical integer",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Abs extends BaseUnaryEvaluation {
        public Abs(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (isInt(a)) {
                return Math.abs((int) a);
            }
            if (isDouble(a)) {
                return Math.abs((double) a);
            }
            if (isLong(a)) {
                return Math.abs((long) a);
            }
            return Math.abs((float) a);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> clazz =  single.checkAndGetReturnType();
            Preconditions.checkArgument(clazz == Integer.TYPE ||
                    clazz == Double.TYPE ||
                    clazz == Long.TYPE ||
                    clazz ==Float.TYPE);
            return clazz;
        }
    }

    @EvaluationInfo(name = "ACOS",
            desc = "returns the arc cosine of a value; the returned " +
                    "angle is in the range 0.0 through pi.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Acos extends BaseUnaryEvaluation {
        public Acos(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.acos((double) single.eval(row));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "add_extract",
            desc = "returns the sum of its arguments, throwing an exception if the result overflows",
            kind = SqlKind.OTHER_FUNCTION)
    public static class AddExtract extends BaseBinaryEvaluation {
        public AddExtract(Evaluation left, Evaluation right) {
            super(left,right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a, b)) {
                return Math.addExact((int) a, (int) b);
            }
            return Math.addExact((long) a, (long) b);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Class<?> b = right.checkAndGetReturnType();
            Preconditions.checkArgument( a == Integer.TYPE && b == Integer.TYPE ||
                    a == Long.TYPE && b == Long.TYPE);
            return a;
        }
    }

    @EvaluationInfo(name = "ASIN",
            desc = "returns the arc sine of a value; " +
                    "the returned angle is in the range -pi/2 through pi/2",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Asin extends BaseUnaryEvaluation{
        public Asin(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.asin((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "ATAN",
            desc = "returns the arc tangent of a value; " +
                    "the returned angle is in the range -pi/2 through pi/2.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Atan extends BaseUnaryEvaluation {
        public Atan(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.atan((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "ATAN2",
            desc = "returns the angle theta from the conversion of rectangular " +
                    "coordinates (x, y) to polar coordinates (r, theta).",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Atan2 extends BaseBinaryEvaluation {
        public Atan2(Evaluation left, Evaluation right) {
            super(left,right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.atan2((double) left.eval(row), (double) right.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(left.checkAndGetReturnType() == Double.TYPE &&
                    right.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "cbrt",
            desc = "returns the cube root of a double value.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Cbrt extends BaseUnaryEvaluation {
        public Cbrt(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.cbrt((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "CEIL",
            desc = "Returns the smallest (closest to negative infinity) double value " +
                    "that is greater than or equal to the argument " +
                    "and is equal to a mathematical integer",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Ceil extends BaseUnaryEvaluation {
        public Ceil(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.ceil((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "COS",
            desc = "returns the trigonometric cosine of an angle",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Cos extends BaseUnaryEvaluation {
        public Cos(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.cos((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "COT",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Cot extends BaseUnaryEvaluation {
        public Cot(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return 1 / Math.tan((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "COSH",
            desc = "returns the hyperbolic cosine of a double value.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Cosh extends BaseUnaryEvaluation {
        public Cosh(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.cosh((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
           Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
           return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "EXP",
            desc = "returns Euler's number e raised to the power of a double value.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Exp extends BaseUnaryEvaluation {
        public Exp(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.exp((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
           Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "FLOOR",
            desc = "the largest (closest to positive infinity) floating-point " +
                    "value that less than or equal to the argument " +
                    "and is equal to a mathematical integer",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Floor extends BaseUnaryEvaluation {
        public Floor(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.floor((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "POW",
            desc = "returns the value of the first " +
                    "argument raised to the power of the second argument",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Pow extends BaseBinaryEvaluation {
        public Pow(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.pow((double) left.eval(row), (double) right.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(left.checkAndGetReturnType() == Double.TYPE &&
                    right.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "RANDOM",
            desc = "returns a double value with a positive sign, " +
                    "greater than or equal to 0.0 and less than 1.0.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Random extends BaseUnaryEvaluation {
        private java.util.Random random;
        public Random(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return random.nextDouble();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
        @Override
        public void open(RuntimeContext context) {
            int seed = (int) single.eval(null);
            if (seed == 0) {
                random = new java.util.Random();
            } else {
                random = new java.util.Random(seed);
            }
        }
        @Override
        public Evaluation eager() {
            return this;
        }
    }

    @EvaluationInfo(name = "RANDOM_INTEGER",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class RandomInteger extends BaseBinaryEvaluation {
        private java.util.Random random = new java.util.Random(System.currentTimeMillis());
        public RandomInteger(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return random.nextInt((int) right.eval(row)) + (int) left.eval(row);
        }
        @Override
        public Evaluation eager() {
            return this;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(left.checkAndGetReturnType() == Integer.TYPE &&
                    right.checkAndGetReturnType() == Integer.TYPE);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "uuid",
            desc = "",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Uuid implements Evaluation {
        public Uuid() {
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return UUID.randomUUID().toString();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return String.class;
        }
    }

    @EvaluationInfo(name = "ROUND",
            desc = "returns the trigonometric sine of an angle",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Round extends BaseBinaryEvaluation {
        public Round(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            if (isInt(a) || isLong(a)) {
                return a;
            }
            if (right == null) {
                if (isDouble(a)) {
                    return Math.round((double) a);
                }
                return Math.round((float) a);
            } else {
                int preciseNum = (int) right.eval(row);
                if (isDouble(a)) {
                    BigDecimal b = new BigDecimal((double) a);
                    return b.round(new MathContext(preciseNum)).doubleValue();
                }
                BigDecimal c = new BigDecimal((float) a);
                return c.round(new MathContext(preciseNum)).floatValue();
            }
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = left.checkAndGetReturnType();
            Preconditions.checkArgument( a == Integer.TYPE ||
                    a == Long.TYPE || a == Double.TYPE || a == Float.TYPE);
            if( right != null ){
                Preconditions.checkArgument( right.checkAndGetReturnType() == Integer.TYPE);
            }
            return a;
        }
    }

    @EvaluationInfo(name = "SIGN",
            desc = "returns the signum function of the argument; zero if the argument is zero," +
                    " 1.0f if the argument is greater than zero, -1.0f if the argument is less than zero",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sign extends BaseUnaryEvaluation {
        public Sign(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object evalResult = single.eval(row);
            if (isDouble(evalResult)) {
                return Math.signum((double) evalResult);
            }
            return Math.signum((float) evalResult);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> clazz = single.checkAndGetReturnType();
            Preconditions.checkArgument(clazz == Double.TYPE || clazz == Float.TYPE);
            return clazz;
        }
    }

    @EvaluationInfo(name = "SIN",
            desc = "returns the trigonometric sine of an angle",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sin extends BaseUnaryEvaluation {
        public Sin(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.sin((double) single.eval(row));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "SINH",
            desc = "Returns the hyperbolic sine of a value",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sinh extends BaseUnaryEvaluation {
        public Sinh(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.sinh((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "SQRT",
            desc = "returns the correctly rounded positive square root of a double value",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sqrt extends BaseUnaryEvaluation {
        public Sqrt(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.sqrt((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "TAN",
            desc = "returns the trigonometric tangent of an angle.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Tan extends BaseUnaryEvaluation {
       public Tan(Evaluation single) {
           super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.tan((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }

    @EvaluationInfo(name = "TANH",
            desc = "returns the hyperbolic tangent of a double value.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Tanh extends BaseUnaryEvaluation {
        public Tanh(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.tanh((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "PI",
            desc = " the ratio of the circumference of a circle to its diameter",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Pi implements Evaluation {
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.PI;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return Double.TYPE;
        }
        @Override
        public Evaluation eager() {
            return new Constant(Math.PI);
        }

        @Override
        public Evaluation eager(Map<String, Object> row) {
            return new Constant(Math.PI);
        }
    }

    @EvaluationInfo(name = "E",
            desc = "the base of the natural logarithms",
            kind = SqlKind.OTHER_FUNCTION)
    public static class E implements Evaluation {
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.E;
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return Double.TYPE;
        }

        @Override
        public Evaluation eager() {
            return new Constant(Math.E);
        }

        @Override
        public Evaluation eager(Map<String, Object> row) {
            return new Constant(Math.E);
        }
    }

    @EvaluationInfo(name = "LOG",
            desc = "returns the natural logarithm of (base on e) value",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Log extends BaseUnaryEvaluation {
        public Log(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.log((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
           Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
           return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "LN",
            desc = "returns the natural logarithm of (base on e) value",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Ln extends BaseUnaryEvaluation {
        public Ln(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.log((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }
    }


    @EvaluationInfo(name = "LOG10",
            desc = "returns the natural logarithm of (base on 10) value",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Log10 extends BaseUnaryEvaluation {

        public Log10(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return Math.log10((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "DEGRESS",
            desc = "converts an angle measured in radians to an approximately" +
                    " equivalent angle measured in degrees",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Degrees extends BaseUnaryEvaluation  {
        public Degrees(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.toDegrees((double) single.eval(row));
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }


    }

    @EvaluationInfo(name = "RADIANS",
            desc = "converts an angle measured in degrees to an approximately" +
                    " equivalent angle measured in radians",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Radians extends BaseUnaryEvaluation {
        public Radians(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return Math.toRadians((double) single.eval(row));
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Double.TYPE);
            return Double.TYPE;
        }

    }

    @EvaluationInfo(name = "bin",
            desc = "returns a string representation of integer in binary format. Returns NULL if integer is NULL.",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Bin extends BaseUnaryEvaluation  {
        public Bin(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            if (isInt(a)) {
                return Integer.toBinaryString((int) a);
            }
            return Long.toBinaryString((long) a);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a == Integer.TYPE || a == Long.TYPE);
            return String.class;
        }
    }

    @EvaluationInfo(name = "hex",
            desc = "returns a string representation of an integer numeric value " +
                    "or a string in hex format. Returns NULL if the argument is NULL",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Hex extends BaseUnaryEvaluation {
        public Hex(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            if (isInt(a)) {
                return Integer.toHexString((int) a);
            }
            if (isLong(a)) {
                return Long.toHexString((long) a);
            }
            if (isFloat(a)) {
                return Float.toHexString((float) a);
            }
            if (isDouble(a)) {
                return Double.toHexString((double) a);
            }
            StringBuilder sb = new StringBuilder();
            for (char c : a.toString().toCharArray()) {
                sb.append(Integer.toHexString((int) c));
            }
            return sb.toString();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> clazz =  single.checkAndGetReturnType();
            Preconditions.checkArgument(clazz == Integer.TYPE ||
                    clazz == Double.TYPE ||
                    clazz == Long.TYPE ||
                    clazz ==Float.TYPE || clazz == String.class);
            return clazz;
        }
    }

    @EvaluationInfo(name = "MIN",
            desc = "returns the smaller of two values",
            kind = SqlKind.MIN)
    public static class Min extends BaseBinaryEvaluation {
        public Min(Evaluation left, Evaluation right) {
           super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isLong(a, b)) {
                return Math.min((long) a, (long) b);
            }
            if (isInt(a, b)) {
                return Math.min((int) a, (int) b);
            }
            if (isDouble(a, b)) {
                return Math.min((double) a, (double) b);
            }
            return Math.min((float) a, (float) b);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = right.checkAndGetReturnType();
            Class<?> b = left.checkAndGetReturnType();
            Preconditions.checkArgument(a == Long.TYPE && b == Long.TYPE ||
                    a == Integer.TYPE && b == Integer.TYPE || a == Double.TYPE && b == Double.TYPE ||
                    a == Float.TYPE && b == Float.TYPE);
            return a;
        }
    }

    @EvaluationInfo(name = "MAX",
            desc = "returns the greater of two values",
            kind = SqlKind.MAX)
    public static class Max extends BaseBinaryEvaluation {
        public Max(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a, b)) {
                return Math.max((int)a, (int) b);
            }
            if (isLong(a, b)) {
                return Math.max((long) a, (long) b);
            }
            if (isDouble(a, b)) {
                return Math.max((double) a, (double) b);
            }
            return Math.max((float) a, (float) b);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = right.checkAndGetReturnType();
            Class<?> b = left.checkAndGetReturnType();
            Preconditions.checkArgument(a == Long.TYPE && b == Long.TYPE ||
                    a == Integer.TYPE && b == Integer.TYPE || a == Double.TYPE && b == Double.TYPE ||
                    a == Float.TYPE && b == Float.TYPE);
            return a;
        }
    }

    @EvaluationInfo(name = "*",
            desc = "'*' operator",
            kind = SqlKind.TIMES)
    public static class Multiply extends BaseBinaryEvaluation {
        public Multiply(Evaluation left, Evaluation right) {
            super(left, right);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a) && isLong(b) || isLong(a) && isInt(b) || isLong(a) && isLong(b)) {
                return (long) a * (long) b;
            }
            if (isInt(a, b)) {
                return (int) a * (int) b;
            }
            BigDecimal c = multiply(toBigDecimal((Number) a), toBigDecimal((Number) b));
            if (isFloat(a) && !isDouble(b) || isFloat(b) && !isDouble(a)) {
                return c.floatValue();
            }
            return c.doubleValue();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return getArithmeticReturnType(left.checkAndGetReturnType(),
                    right.checkAndGetReturnType());
        }
    }

    @EvaluationInfo(name = "/",
            desc = "'/' operator",
            kind = SqlKind.DIVIDE)
    public static class Divide extends BaseBinaryEvaluation {
        public Divide(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a) && isLong(b) || isLong(a) && isInt(b) || isLong(a) && isLong(b)) {
                return (long) a / (long) b;
            }
            if (isInt(a, b)) {
                return (int) a / (int) b;
            }
            BigDecimal c = divide(toBigDecimal((Number) a), toBigDecimal((Number) a));
            if (isFloat(a) && !isDouble(b) || isFloat(b) && !isDouble(a)) {
                return c.floatValue();
            }
            return c.doubleValue();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return getArithmeticReturnType(left.checkAndGetReturnType(),
                    right.checkAndGetReturnType());
        }
    }

    @EvaluationInfo(name = "-",
            desc = "'-' operator",
            kind = SqlKind.MINUS)
    public static class Minus extends BaseBinaryEvaluation {
        public Minus(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a) && isLong(b) || isLong(a) && isInt(b) || isLong(a) && isLong(b)) {
                return (long) a - (long) b;
            }
            if (isInt(a, b)) {
                return (int) a - (int) b;
            }
            BigDecimal c = minus(toBigDecimal((Number) a), toBigDecimal((Number) a));
            if (isFloat(a) && !isDouble(b) || isFloat(b) && !isDouble(a)) {
                return c.floatValue();
            }
            return c.doubleValue();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return getArithmeticReturnType(left.checkAndGetReturnType(),
                    right.checkAndGetReturnType());
        }
    }

    @EvaluationInfo(name = "+",
            desc = "'+' operator",
            kind = SqlKind.PLUS)
    public static class Plus extends BaseBinaryEvaluation {
        public Plus(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a) && isLong(b) || isLong(a) && isInt(b) || isLong(a) && isLong(b)) {
                return (long) a + (long) b;
            }
            if (isInt(a, b)) {
                return (int) a + (int) b;
            }
            BigDecimal c = plus(toBigDecimal((Number) a), toBigDecimal((Number) a));
            if (isFloat(a) && !isDouble(b) || isFloat(b) && !isDouble(a)) {
                return c.floatValue();
            }
            return c.doubleValue();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return getArithmeticReturnType(left.checkAndGetReturnType(),
                    right.checkAndGetReturnType());
        }
    }


    @EvaluationInfo(name = "MOD",
            desc = "mod operator",
            kind = SqlKind.MOD)
    public static class Mod extends BaseBinaryEvaluation {
        public Mod(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = left.eval(row);
            Object b = right.eval(row);
            if (isInt(a) && isLong(b) || isLong(a) && isInt(b) || isLong(a) && isLong(b)) {
                return (long) a % (long) b;
            }
            if (isInt(a, b)) {
                return (int) a % (int) b;
            }
            BigDecimal c = mod(toBigDecimal((Number) a), toBigDecimal((Number) a));
            if (isFloat(a) && !isDouble(b) || isFloat(b) && !isDouble(a)) {
                return c.floatValue();
            }
            return c.doubleValue();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            return getArithmeticReturnType(left.checkAndGetReturnType(),
                    right.checkAndGetReturnType());
        }
    }


    @EvaluationInfo(name = "-",
            desc = "'negative' operator",
            kind = SqlKind.MINUS_PREFIX)
    public static class Negative extends BaseUnaryEvaluation {
        public Negative(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (isDouble(a)) {
                return -(double) a;
            }
            if (isFloat(a)) {
                return -(float) a;
            }
            if (isInt(a)) {
                return Math.negateExact((int) a);
            }
            return Math.negateExact((long) a);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> clazz =  single.checkAndGetReturnType();
            Preconditions.checkArgument(clazz == Integer.TYPE ||
                    clazz == Double.TYPE ||
                    clazz == Long.TYPE ||
                    clazz ==Float.TYPE);
            return clazz;
        }
    }

    @EvaluationInfo(name = "+",
            desc = "'positive' operator",
            kind = SqlKind.PLUS_PREFIX)
    public static class Positive extends BaseUnaryEvaluation {
        public Positive(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return single.eval(row);
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> clazz =  single.checkAndGetReturnType();
            Preconditions.checkArgument(clazz == Integer.TYPE ||
                    clazz == Double.TYPE ||
                    clazz == Long.TYPE ||
                    clazz ==Float.TYPE);
            return clazz;
        }
    }
}
