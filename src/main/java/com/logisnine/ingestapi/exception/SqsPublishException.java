package com.logisnine.ingestapi.exception;

public class SqsPublishException extends RuntimeException {

    public SqsPublishException(String message, Throwable cause) {

        super(message, cause);
    }
}