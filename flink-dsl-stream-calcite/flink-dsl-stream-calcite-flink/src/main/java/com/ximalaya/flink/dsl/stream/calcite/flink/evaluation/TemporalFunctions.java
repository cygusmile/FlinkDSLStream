package com.ximalaya.flink.dsl.stream.calcite.flink.evaluation;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/1
 **/

public class TemporalFunctions {
    @EvaluationInfo(name = "current_timestamp",
            desc = "returns the current SQL timestamp in the UTC time zone",
            kind = SqlKind.OTHER_FUNCTION)
    public static class CurrentTimestamp implements Evaluation{
        @Override
        public Object eval(Map<String, Object> row) {
            return Date.from(Instant.now());
        }

        @Override
        public Class<?> checkAndGetReturnType() {
            return Date.class;
        }
    }

    @EvaluationInfo(name = "year",
            desc = "returns the year from SQL date date. Equivalent to EXTRACT(YEAR FROM date)",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Year extends BaseUnaryEvaluation{
        public Year(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return ((Date) single.eval(row)).toLocalDate().getYear();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a == Date.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "month",
            desc = "returns the month of a year (an integer between 1 and 12) from SQL date date. Equivalent to EXTRACT(MONTH FROM date)",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Month extends BaseUnaryEvaluation{
        public Month(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return ((Date) single.eval(row)).toLocalDate().getMonthValue();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a == Date.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "hour",
            desc = "returns the hour of a day (an integer between 0 and 23) from SQL timestamp timestamp. Equivalent to EXTRACT(HOUR FROM timestamp)",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Hour extends BaseUnaryEvaluation{
        public Hour(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return LocalDateTime.ofInstant(((Date) single.eval(row)).toInstant(), ZoneId.systemDefault()).getHour();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a == Date.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "minute",
            desc = "returns the minute of an hour (an integer between 0 and 59) from SQL timestamp timestamp. Equivalent to EXTRACT(MINUTE FROM timestamp)",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Minute extends BaseUnaryEvaluation{
        public Minute(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return LocalDateTime.ofInstant(((Date) single.eval(row)).toInstant(), ZoneId.systemDefault()).getMinute();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a == Date.class);
            return Integer.TYPE;
        }
    }

    @EvaluationInfo(name = "second",
            desc = "returns the second of a minute (an integer between 0 and 59) from SQL timestamp. Equivalent to EXTRACT(SECOND FROM timestamp)",
            kind = SqlKind.OTHER_FUNCTION)
    public static class Second extends BaseUnaryEvaluation{
        private ZoneId zoneId = ZoneId.systemDefault();
        public Second(Evaluation single) {
            super(single);
        }
        @Override
        public Object eval(Map<String, Object> row) {
            return LocalDateTime.ofInstant(((Date) single.eval(row)).toInstant(),zoneId).getSecond();
        }
        @Override
        public Class<?> checkAndGetReturnType() {
            Class<?> a = single.checkAndGetReturnType();
            Preconditions.checkArgument(a == Date.class);
            return Integer.TYPE;
        }
    }

}
