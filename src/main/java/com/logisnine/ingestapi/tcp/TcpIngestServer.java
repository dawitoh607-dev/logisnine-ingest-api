package com.logisnine.ingestapi.tcp;

import com.logisnine.ingestapi.dto.RawTcpMessage;
import com.logisnine.ingestapi.service.SqsPublishService;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import java.time.Instant;

import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Slf4j
@Component
@RequiredArgsConstructor
public class TcpIngestServer {

    private final SqsPublishService sqsPublishService;

    private final RawPacketValidator rawPacketValidator;


    @Value("${tcp.ingest.port:9000}")
    private int port;


    @Value("${tcp.ingest.read-timeout-ms:15000}")
    private int readTimeoutMs;


    @Value("${tcp.ingest.max-packet-bytes:65571}")
    private int maxPacketBytes;


    /*
     * 공개 TCP 포트이므로 무제한 Thread / Queue를 사용하지 않는다.
     *
     * core       : 10
     * max        : 50
     * queue      : 100
     *
     * 초과 연결은 즉시 거절.
     */
    private final ExecutorService clientExecutor = new ThreadPoolExecutor(10, 50, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.AbortPolicy());


    private ServerSocket serverSocket;

    private volatile boolean running = true;


    /*
     * =====================================================
     * TCP Server Start
     * =====================================================
     */
    @PostConstruct
    public void start() {

        Thread serverThread = new Thread(this::acceptLoop);

        serverThread.setName("tcp-ingest-accept");

        serverThread.setDaemon(true);

        serverThread.start();
    }


    /*
     * =====================================================
     * Connection Accept
     * =====================================================
     */
    private void acceptLoop() {

        try {

            serverSocket = new ServerSocket(port);


            log.info("======================================");

            log.info("TCP INGEST SERVER STARTED");

            log.info("PORT = {}", port);

            log.info("READ TIMEOUT = {} ms", readTimeoutMs);

            log.info("MAX PACKET = {} bytes", maxPacketBytes);

            log.info("======================================");


            while (running) {

                Socket socket = serverSocket.accept();


                String remoteAddress = socket.getInetAddress().getHostAddress();


                int remotePort = socket.getPort();


                log.info("[TCP CONNECT] remote={}:{}", remoteAddress, remotePort);


                try {

                    clientExecutor.submit(() -> handleClient(socket));

                } catch (RejectedExecutionException e) {

                    /*
                     * Thread/Queue가 가득 찬 경우
                     * 연결을 바로 종료한다.
                     */
                    log.warn("[TCP REJECTED] remote={}:{}", remoteAddress, remotePort);


                    closeQuietly(socket);
                }
            }


        } catch (SocketException e) {

            /*
             * 서버 종료 과정에서 ServerSocket.close()가 호출되면
             * SocketException이 발생할 수 있음.
             */
            if (running) {

                log.error("TCP server socket error", e);
            }


        } catch (Exception e) {

            log.error("TCP server error", e);
        }
    }


    /*
     * =====================================================
     * Client Handling
     * =====================================================
     */
    private void handleClient(Socket socket) {


        String remoteAddress = socket.getInetAddress().getHostAddress();


        int remotePort = socket.getPort();


        /*
         * 매우 중요.
         *
         * Framer는 Connection마다 하나씩 생성해야 한다.
         *
         * TCP Stream의 미완성 Packet 상태를 보관하기 때문에
         * 여러 Gateway가 하나의 Framer를 공유하면 안 된다.
         */
        TcpPacketFramer packetFramer = new TcpPacketFramer(maxPacketBytes);


        try (socket; InputStream inputStream = socket.getInputStream()) {

            /*
             * 접속만 하고 데이터를 보내지 않는
             * Scanner / Bot 연결 방어.
             */
            socket.setSoTimeout(readTimeoutMs);


            byte[] buffer = new byte[4096];


            while (running) {

                int read;


                try {

                    read = inputStream.read(buffer);

                } catch (SocketTimeoutException e) {

                    log.info("[TCP TIMEOUT] remote={}:{}", remoteAddress, remotePort);

                    break;
                }


                /*
                 * Client가 정상적으로 Connection 종료
                 */
                if (read == -1) {
                    break;
                }


                /*
                 * 실제 읽은 만큼만 복사
                 */
                byte[] chunk = Arrays.copyOf(buffer, read);


                /*
                 * RAW chunk 전체를 INFO로 찍으면
                 * 로그 공격 / 로그 용량 문제가 생길 수 있으므로 DEBUG.
                 */
                log.debug("[TCP RECEIVE] remote={}:{}, bytes={}", remoteAddress, remotePort, read);


                /*
                 * TCP Stream → 완전한 Packet 단위로 분리
                 */
                List<byte[]> packets = packetFramer.append(chunk);


                /*
                 * Packet 하나씩 검증 후 SQS 전송
                 */
                for (byte[] packet : packets) {

                    processPacket(socket, packet);
                }
            }


        } catch (Exception e) {

            log.error("[TCP ERROR] remote={}:{}", remoteAddress, remotePort, e);


        } finally {

            log.info("[TCP DISCONNECT] remote={}:{}", remoteAddress, remotePort);
        }
    }


    /*
     * =====================================================
     * Packet Processing
     * =====================================================
     */
    private void processPacket(Socket socket, byte[] packet) {


        String remoteAddress = socket.getInetAddress().getHostAddress();


        int remotePort = socket.getPort();


        /*
         * -------------------------------------------------
         * 1. 최소 프로토콜 검증
         *
         * 우리 장비 Packet 형태가 아니면
         * 절대로 SQS에 넣지 않는다.
         *
         * 여기서 온도/GPS/Sensor ID 등의 의미를
         * 해석하지는 않는다.
         * -------------------------------------------------
         */
        if (!rawPacketValidator.isValid(packet)) {

            log.warn("[TCP INVALID PACKET] remote={}:{}, bytes={}", remoteAddress, remotePort, packet.length);


            log.debug("[TCP INVALID HEX] remote={}:{}, hex={}", remoteAddress, remotePort, HexFormat.of().withUpperCase().formatHex(packet));


            return;
        }


        /*
         * -------------------------------------------------
         * 2. Raw Packet → Base64
         * -------------------------------------------------
         */
        String base64Payload = Base64.getEncoder().encodeToString(packet);


        /*
         * -------------------------------------------------
         * 3. SQS Message 생성
         * -------------------------------------------------
         */
        RawTcpMessage message = new RawTcpMessage("TCP", Instant.now(), remoteAddress, remotePort, packet.length, "BASE64", base64Payload);


        /*
         * -------------------------------------------------
         * 4. SQS 전송
         * -------------------------------------------------
         */
        try {

            String messageId = sqsPublishService.publishRawTcp(message);


            log.info("[TCP -> SQS] remote={}:{}, packetBytes={}, messageId={}", remoteAddress, remotePort, packet.length, messageId);


            /*
             * 개발 중 Packet 확인용.
             * 운영에서는 DEBUG 유지.
             */
            log.debug("[TCP PACKET HEX] remote={}:{}, hex={}", remoteAddress, remotePort, HexFormat.of().withUpperCase().formatHex(packet));


            /*
             * TODO
             *
             * 하드웨어팀으로부터 ACK 규격을 받으면
             * SQS 전송 성공 이후 여기서 ACK를 내려준다.
             *
             * ex)
             *
             * OutputStream out = socket.getOutputStream();
             * out.write(...);
             * out.flush();
             */


        } catch (Exception e) {

            /*
             * SQS 실패 시 Packet을 성공 처리하지 않는다.
             *
             * ACK 프로토콜이 추가되더라도
             * 여기서는 ACK를 보내지 않아야 함.
             */
            log.error("[TCP -> SQS FAILED] remote={}:{}, packetBytes={}", remoteAddress, remotePort, packet.length, e);
        }
    }


    /*
     * =====================================================
     * Shutdown
     * =====================================================
     */
    @PreDestroy
    public void shutdown() {

        running = false;


        log.info("TCP INGEST SERVER STOPPING...");


        try {

            if (serverSocket != null) {

                serverSocket.close();
            }

        } catch (Exception ignored) {
        }


        clientExecutor.shutdown();


        try {

            if (!clientExecutor.awaitTermination(5, TimeUnit.SECONDS)) {

                clientExecutor.shutdownNow();
            }

        } catch (InterruptedException e) {

            clientExecutor.shutdownNow();

            Thread.currentThread().interrupt();
        }


        log.info("TCP INGEST SERVER STOPPED");
    }


    /*
     * =====================================================
     * Utility
     * =====================================================
     */
    private void closeQuietly(Socket socket) {

        try {

            socket.close();

        } catch (Exception ignored) {
        }
    }
}