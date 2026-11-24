package com.logisnine.ingestapi.dto;

import java.time.Instant;

public record RawTcpMessage(
        String source,
        Instant receivedAt,
        String remoteAddress,
        int remotePort,
        int packetLength,
        String payloadEncoding,
        String payload
) {
}