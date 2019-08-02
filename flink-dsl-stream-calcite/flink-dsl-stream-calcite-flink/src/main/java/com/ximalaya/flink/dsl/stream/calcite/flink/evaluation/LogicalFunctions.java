package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;

import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/2
 **/

public class LogicalFunctions {

    @EvaluationInfo(name = "AND",
            desc = "and operator",
            kind = SqlKind.AND)
    public static class And extends BaseBinaryEvaluation {
        public And(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return (boolean) left.eval(row) && (boolean) right.eval(row);
        }


        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(left.checkAndGetReturnType() == Boolean.TYPE &&
                    right.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }

        @Override
        public Evaluation eager() {
            this.left = left.eager();
            this.right = right.eager();

            if (left instanceof Constant && right instanceof Constant) {
                return new Constant(eval(null));
            } else if (left instanceof Constant) {
                if (!(boolean) left.eval(null)) {
                    return new Constant(false);
                } else {
                    return right;
                }
            } else if (right instanceof Constant) {
                if (!(boolean) right.eval(null)) {
                    return new Constant(false);
                } else {
                    return left;
                }
            } else {
                return this;
            }
        }

        @Override
        public Evaluation eager(Map<String, Object> row) {
            this.left = left.eager(row);
            this.right = right.eager(row);

            if (left instanceof Constant && right instanceof Constant) {
                return new Constant(eval(row));
            } else if (left instanceof Constant) {
                if (!(boolean) left.eval(row)) {
                    return new Constant(false);
                } else {
                    return right;
                }
            } else if (right instanceof Constant) {
                if (!(boolean) right.eval(row)) {
                    return new Constant(false);
                } else {
                    return left;
                }
            } else {
                return this;
            }
        }
    }

    @EvaluationInfo(name = "OR",
            desc = "or operator",
            kind = SqlKind.OR)
    public static class OR extends BaseBinaryEvaluation {
        public OR(Evaluation left, Evaluation right) {
            super(left, right);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return (boolean) left.eval(row) || (boolean) right.eval(row);
        }

        @Override
        public Evaluation eager() {
            this.left = left.eager();
            this.right = right.eager();

            if (left instanceof Constant && right instanceof Constant) {
                return new Constant(eval(null));
            } else if (left instanceof Constant) {
                if ((boolean) left.eval(null)) {
                    return new Constant(true);
                } else {
                    return right;
                }
            } else if (right instanceof Constant) {
                if ((boolean) right.eval(null)) {
                    return new Constant(true);
                } else {
                    return left;
                }
            } else {
                return this;
            }
        }

        @Override
        public Evaluation eager(Map<String, Object> row) {
            this.left = left.eager(row);
            this.right = right.eager(row);

            if (left instanceof Constant && right instanceof Constant) {
                return new Constant(eval(row));
            } else if (left instanceof Constant) {
                if ((boolean) left.eval(row)) {
                    return new Constant(true);
                } else {
                    return right;
                }
            } else if (right instanceof Constant) {
                if ((boolean) right.eval(row)) {
                    return new Constant(true);
                } else {
                    return left;
                }
            } else {
                return this;
            }
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(left.checkAndGetReturnType() == Boolean.TYPE &&
                    right.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "NOT",
            desc = "not operator",
            kind = SqlKind.NOT)
    public static class Not extends BaseUnaryEvaluation {
        public Not(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return !(boolean) single.eval(row);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS TRUE",
            desc = "is true operator",
            kind = SqlKind.IS_TRUE)
    public static class IsTrue extends BaseUnaryEvaluation {
        public IsTrue(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return single.eval(row);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS NOT TRUE",
            desc = "is not true operator",
            kind = SqlKind.IS_NOT_TRUE)
    public static class IsNotTrue extends BaseUnaryEvaluation {
        public IsNotTrue(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return !(boolean) single.eval(row);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS FALSE",
            desc = "is false operator",
            kind = SqlKind.IS_FALSE)
    public static class IsFalse extends BaseUnaryEvaluation {
        public IsFalse(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return !(boolean) single.eval(row);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }
    }

    @EvaluationInfo(name = "IS NOT FALSE",
            desc = "is not false operator",
            kind = SqlKind.IS_NOT_FALSE)
    public static class IsNotFalse extends BaseUnaryEvaluation {
        public IsNotFalse(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            return single.eval(row);
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Preconditions.checkArgument(single.checkAndGetReturnType() == Boolean.TYPE);
            return Boolean.TYPE;
        }
    }
}
