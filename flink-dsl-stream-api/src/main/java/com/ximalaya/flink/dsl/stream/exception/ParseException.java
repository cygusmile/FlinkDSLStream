package com.ximalaya.flink.dsl.stream.exception;

/**
 * @author martin.dong
 * @mail martin.dong@ximalaya.com
 * @date 2019/6/23
 **/

public class ParseException extends RuntimeException {

    public ParseException() {
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParseException(String message) {
        super(message);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }

}
