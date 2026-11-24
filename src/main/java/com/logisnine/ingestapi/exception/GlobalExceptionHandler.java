package com.logisnine.ingestapi.exception;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(SqsPublishException.class)
    public ResponseEntity<Map<String, String>> handleSqsPublish(SqsPublishException e) {

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("status", "SERVICE_UNAVAILABLE",

                "message", "메시지 처리 서버가 일시적으로 사용할 수 없습니다."));
    }
}