package com.logisnine.ingestapi.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.logisnine.ingestapi.dto.RawTcpMessage;
import com.logisnine.ingestapi.exception.SqsPublishException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import tools.jackson.databind.ObjectMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class SqsPublishService {

    private final SqsClient sqsClient;

    private final ObjectMapper objectMapper;


    @Value("${aws.sqs.queue-url}")
    private String queueUrl;


    /*
     * 기존 HTTP ingest용
     */
    public String publish(String rawBody) {

        return send(rawBody);
    }


    /*
     * TCP raw packet용
     */
    public String publishRawTcp(RawTcpMessage message) {

        final String json;

        try {

            json = objectMapper.writeValueAsString(message);

        } catch (Exception e) {

            log.error("TCP Raw 메시지 JSON 변환 실패", e);

            throw new SqsPublishException("TCP 메시지 JSON 변환에 실패했습니다.", e);
        }


        return send(json);
    }


    private String send(String messageBody) {

        try {

            SendMessageRequest request = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody).build();


            SendMessageResponse response = sqsClient.sendMessage(request);


            return response.messageId();


        } catch (SqsException e) {

            log.error("SQS 메시지 전송 실패", e);


            throw new SqsPublishException("메시지 큐 전송에 실패했습니다.", e);
        }
    }
}