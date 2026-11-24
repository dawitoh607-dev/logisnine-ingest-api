package com.logisnine.ingestapi.tcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TcpPacketFramer {

    /*
     * 현재 실제 수신 데이터 기준.
     *
     * 첫 Byte:
     * 0x21 = Header 길이 33 byte
     *
     * Header Length byte까지 포함하면
     * 실제 Header 전체는 34 byte.
     */
    private static final int EXPECTED_HEADER_LENGTH = 0x21;

    /*
     * 현재 실제 Format
     */
    private static final int EXPECTED_FORMAT = 0x01;

    /*
     * Packet 마지막 CRC
     */
    private static final int CRC_LENGTH = 2;


    private final int maxPacketBytes;


    private byte[] pending = new byte[0];


    public TcpPacketFramer(int maxPacketBytes) {

        this.maxPacketBytes = maxPacketBytes;
    }


    public List<byte[]> append(byte[] incoming) {

        appendBuffer(incoming);

        List<byte[]> packets = new ArrayList<>();


        while (true) {

            /*
             * Header Length + Format까지 필요
             */
            if (pending.length < 2) {
                break;
            }


            int headerLength = Byte.toUnsignedInt(pending[0]);


            int format = Byte.toUnsignedInt(pending[1]);


            /*
             * 지금 실제 사용하는 Format만 허용
             */
            if (format != 0x01) {

                discardOneByte();

                continue;
            }


            /*
             * 첫 length byte 포함
             */
            int headerTotalBytes = 1 + headerLength;


            /*
             * 말도 안 되는 Header 방어
             */
            if (headerTotalBytes < 4 || headerTotalBytes > 256) {

                discardOneByte();

                continue;
            }


            /*
             * Header 전체가 아직 안 들어옴
             */
            if (pending.length < headerTotalBytes) {
                break;
            }


            /*
             * Header 마지막 2 bytes = Body Length
             */
            int bodyLengthOffset = headerTotalBytes - 2;


            int bodyLength = ((pending[bodyLengthOffset] & 0xFF) << 8) | (pending[bodyLengthOffset + 1] & 0xFF);


            int packetLength = headerTotalBytes + bodyLength + 2; // CRC


            /*
             * 프로토콜상 Body Length가 2 byte이므로
             * 이론상 최대치를 넘는 경우만 방어
             */
            if (packetLength > maxPacketBytes) {

                discardOneByte();

                continue;
            }


            /*
             * 아직 완전한 Packet이 안 들어옴
             */
            if (pending.length < packetLength) {
                break;
            }


            byte[] packet = Arrays.copyOfRange(pending, 0, packetLength);


            packets.add(packet);


            pending = Arrays.copyOfRange(pending, packetLength, pending.length);
        }


        return packets;
    }


    private void appendBuffer(byte[] incoming) {

        byte[] merged = new byte[pending.length + incoming.length];


        System.arraycopy(pending, 0, merged, 0, pending.length);


        System.arraycopy(incoming, 0, merged, pending.length, incoming.length);


        pending = merged;
    }


    private void discardOneByte() {

        pending = Arrays.copyOfRange(pending, 1, pending.length);
    }
}