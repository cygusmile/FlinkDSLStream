package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.security.MessageDigest;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/

public class HashFunctions {

    @EvaluationInfo(name = "MD5",
            desc = "md5 compute",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Md5 extends BaseUnaryEvaluation {
        private transient MessageDigest md5;
        public Md5(Evaluation single){
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return md5.digest(a.toString().getBytes());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception {
            super.open(context);
            md5 = MessageDigest.getInstance("MD5");
        }
    }

    @EvaluationInfo(name = "SHA1",
            desc = "sha1 compute",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sha1 extends BaseUnaryEvaluation {
        private transient MessageDigest sha1;

        public Sha1(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return sha1.digest(a.toString().getBytes());
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception{
            super.open(context);
            sha1 = MessageDigest.getInstance("SHA1");
        }
    }

    @EvaluationInfo(name = "SHA2",
            desc = "sha2 compute",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sha2 extends BaseUnaryEvaluation {
        private transient MessageDigest sha2;
        public Sha2(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return sha2.digest(a.toString().getBytes());
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception{
            super.open(context);
            sha2 = MessageDigest.getInstance("SHA2");
        }
    }

    @EvaluationInfo(name = "SHA224",
            desc = "sha224 compute",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sha224 extends BaseUnaryEvaluation {
        private transient MessageDigest sha224;
        public Sha224(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return sha224.digest(a.toString().getBytes());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception{
            super.open(context);
            sha224 = MessageDigest.getInstance("SHA224");
        }
    }

    @EvaluationInfo(name = "SHA256",
            desc = "sha256 compute",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sha256 extends BaseUnaryEvaluation {
        private transient MessageDigest sha256;
        public Sha256(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return sha256.digest(a.toString().getBytes());
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }

        @Override
        public void open(RuntimeContext context) throws Exception{
            super.open(context);
            sha256 = MessageDigest.getInstance("SHA256");
        }

    }

    @EvaluationInfo(name = "SHA384",
            desc = "compute sha384",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sha384 extends BaseUnaryEvaluation {
        private transient MessageDigest sha384;
        public Sha384(Evaluation single) {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return sha384.digest(a.toString().getBytes());
        }

        @Override
        public void open(RuntimeContext context) throws Exception{
            super.open(context);
            sha384 = MessageDigest.getInstance("SHA384");
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }
    }

    @EvaluationInfo(name = "SHA512",
            desc = "compute sha512",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Sha512 extends BaseUnaryEvaluation {
        private transient MessageDigest sha512;
        public Sha512(Evaluation single) throws Exception {
            super(single);
        }

        @Override
        public Object eval(Map<String, Object> row) {
            Object a = single.eval(row);
            if (a == null) {
                return null;
            }
            return sha512.digest(a.toString().getBytes());
        }
        @Override
        public void open(RuntimeContext context) throws Exception{
            super.open(context);
            sha512 = MessageDigest.getInstance("SHA512");
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument( a == String.class );
            return String.class;
        }
    }
}
