package com.rutomq.flink;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;

final class KafkaWire {
    private static final int MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

    private KafkaWire() {}

    static ByteBuffer exchange(
            String bootstrap, AbstractRequest request, int correlationId) throws IOException {
        return exchange(
                bootstrap,
                request,
                correlationId,
                "rutomq-wire-smoke");
    }

    static ByteBuffer exchange(
            String bootstrap,
            AbstractRequest request,
            int correlationId,
            String clientId)
            throws IOException {
        RequestHeader requestHeader =
                new RequestHeader(
                        request.apiKey(),
                        request.version(),
                        clientId,
                        correlationId);
        return exchangePayload(
                bootstrap,
                request.apiKey(),
                request.version(),
                correlationId,
                request.serializeWithHeader(requestHeader));
    }

    static ByteBuffer exchangeRaw(
            String bootstrap,
            ApiKeys apiKey,
            short version,
            Message data,
            int correlationId)
            throws IOException {
        RequestHeader requestHeader =
                new RequestHeader(
                        apiKey, version, "rutomq-raw-wire-smoke", correlationId);
        ByteBuffer payload =
                RequestUtils.serialize(
                        requestHeader.data(),
                        requestHeader.headerVersion(),
                        data,
                        version);
        return exchangePayload(
                bootstrap, apiKey, version, correlationId, payload);
    }

    private static ByteBuffer exchangePayload(
            String bootstrap,
            ApiKeys apiKey,
            short version,
            int correlationId,
            ByteBuffer payload)
            throws IOException {
        Endpoint endpoint = Endpoint.parse(bootstrap);
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(endpoint.host(), endpoint.port()), 10_000);
            socket.setSoTimeout(15_000);
            byte[] requestBytes = new byte[payload.remaining()];
            payload.get(requestBytes);

            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(requestBytes.length);
            output.write(requestBytes);
            output.flush();

            DataInputStream input = new DataInputStream(socket.getInputStream());
            int responseSize = input.readInt();
            if (responseSize < Integer.BYTES || responseSize > MAX_RESPONSE_BYTES) {
                throw new IOException("invalid Kafka response size " + responseSize);
            }
            byte[] responseBytes = input.readNBytes(responseSize);
            if (responseBytes.length != responseSize) {
                throw new EOFException(
                        "Kafka response ended at "
                                + responseBytes.length
                                + " of "
                                + responseSize
                                + " bytes");
            }
            ByteBuffer response = ByteBuffer.wrap(responseBytes);
            short headerVersion = apiKey.responseHeaderVersion(version);
            ResponseHeader responseHeader =
                    ResponseHeader.parse(response, headerVersion);
            if (responseHeader.correlationId() != correlationId) {
                throw new IOException(
                        "unexpected correlation id "
                                + responseHeader.correlationId()
                                + ", expected "
                                + correlationId);
            }
            return response.slice();
        }
    }

    private record Endpoint(String host, int port) {
        static Endpoint parse(String bootstrap) {
            String address = bootstrap.split(",", 2)[0].trim();
            int separator = address.lastIndexOf(':');
            if (separator <= 0 || separator == address.length() - 1) {
                throw new IllegalArgumentException(
                        "bootstrap must contain host:port: " + bootstrap);
            }
            String host = address.substring(0, separator);
            if (host.startsWith("[") && host.endsWith("]")) {
                host = host.substring(1, host.length() - 1);
            }
            return new Endpoint(
                    host, Integer.parseInt(address.substring(separator + 1)));
        }
    }
}
