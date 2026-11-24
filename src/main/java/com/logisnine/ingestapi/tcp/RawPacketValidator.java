package com.logisnine.ingestapi.tcp;

import org.springframework.stereotype.Component;

@Component
public class RawPacketValidator {

    private static final int EXPECTED_FORMAT = 0x01;

    private static final int CRC_LENGTH = 2;


    public boolean isValid(byte[] packet) {

        if (packet == null || packet.length < 4) {
            return false;
        }


        /*
         * 첫 Byte = Header Length
         *
         * 현재 실제 데이터:
         * 0x21 = 33
         *
         * Length byte 자신까지 포함하면
         * Header 전체 = 34 byte
         */
        int headerLength = Byte.toUnsignedInt(packet[0]);

        int headerTotalBytes = 1 + headerLength;


        /*
         * 최소한 Header + CRC는 있어야 함
         */
        if (packet.length < headerTotalBytes + CRC_LENGTH) {

            return false;
        }


        /*
         * 현재 실제 Format
         */
        int format = Byte.toUnsignedInt(packet[1]);

        if (format != EXPECTED_FORMAT) {
            return false;
        }


        /*
         * Header 마지막 2 byte = Body Length
         */
        int bodyLengthOffset = headerTotalBytes - 2;


        if (bodyLengthOffset < 0 || bodyLengthOffset + 1 >= packet.length) {

            return false;
        }


        int bodyLength = ((packet[bodyLengthOffset] & 0xFF) << 8) | (packet[bodyLengthOffset + 1] & 0xFF);


        /*
         * 패킷 전체 길이 계산
         *
         * Body 길이는 고정값이 아님.
         */
        int expectedPacketLength = headerTotalBytes + bodyLength + CRC_LENGTH;


        return packet.length == expectedPacketLength;
    }
}