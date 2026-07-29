package com.rutomq.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

public final class LegacySaslHandshakeV0Smoke {
    private LegacySaslHandshakeV0Smoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "LegacySaslHandshakeV0Smoke <host> <port> <certificate> <mechanism> <username> <password>");
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String mechanism = args[3];
        String username = args[4];
        String password = args[5];
        ScramAlgorithms algorithms = ScramAlgorithms.forMechanism(mechanism);

        try (SSLSocket socket =
                (SSLSocket)
                        sslContext(Path.of(args[2]))
                                .getSocketFactory()
                                .createSocket(host, port)) {
            socket.setSoTimeout(10_000);
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(parameters);
            socket.startHandshake();

            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            writePacket(output, handshakeRequest(mechanism));
            verifyHandshake(readPacket(input), mechanism);

            String nonce =
                    Base64.getEncoder()
                            .withoutPadding()
                            .encodeToString(randomBytes(18));
            String clientFirstBare = "n=" + username + ",r=" + nonce;
            writePacket(
                    output,
                    ("n,," + clientFirstBare).getBytes(StandardCharsets.UTF_8));
            String serverFirst =
                    new String(readPacket(input), StandardCharsets.UTF_8);
            ScramExchange exchange =
                    clientFinal(
                            algorithms,
                            password,
                            clientFirstBare,
                            serverFirst);
            writePacket(
                    output,
                    exchange.clientFinal().getBytes(StandardCharsets.UTF_8));
            verifyServerFinal(
                    readPacket(input), exchange.expectedServerSignature());

            writePacket(output, metadataRequest());
            ByteBuffer metadata = ByteBuffer.wrap(readPacket(input));
            if (metadata.getInt() != 2 || metadata.getInt() != 1) {
                throw new AssertionError(
                        "authenticated Metadata response was malformed");
            }
        }
        System.out.printf("%s SaslHandshake v0 opaque SCRAM passed%n", mechanism);
    }

    private static byte[] handshakeRequest(String mechanism) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(17);
            output.writeShort(0);
            output.writeInt(1);
            writeString(output, "rutomq-sasl-v0");
            writeString(output, mechanism);
        }
        return bytes.toByteArray();
    }

    private static void verifyHandshake(byte[] payload, String mechanism) {
        ByteBuffer response = ByteBuffer.wrap(payload);
        if (response.getInt() != 1 || response.getShort() != 0) {
            throw new AssertionError("SaslHandshake v0 failed");
        }
        int count = response.getInt();
        boolean found = false;
        for (int index = 0; index < count; index++) {
            found |= readString(response).equals(mechanism);
        }
        if (!found || response.hasRemaining()) {
            throw new AssertionError(
                    "SaslHandshake v0 mechanism list was malformed");
        }
    }

    private static byte[] metadataRequest() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeShort(3);
            output.writeShort(1);
            output.writeInt(2);
            writeString(output, "rutomq-sasl-v0");
            output.writeInt(-1);
        }
        return bytes.toByteArray();
    }

    private static ScramExchange clientFinal(
            ScramAlgorithms algorithms,
            String password,
            String clientFirstBare,
            String serverFirst)
            throws Exception {
        Map<String, String> attributes = attributes(serverFirst);
        byte[] salt = Base64.getDecoder().decode(required(attributes, "s"));
        int iterations = Integer.parseInt(required(attributes, "i"));
        String withoutProof = "c=biws,r=" + required(attributes, "r");
        String authMessage =
                clientFirstBare + "," + serverFirst + "," + withoutProof;
        PBEKeySpec key =
                new PBEKeySpec(
                        password.toCharArray(),
                        salt,
                        iterations,
                        algorithms.bits());
        byte[] salted =
                SecretKeyFactory.getInstance(algorithms.pbkdf2())
                        .generateSecret(key)
                        .getEncoded();
        byte[] clientKey =
                hmac(algorithms.hmac(), salted, "Client Key".getBytes(StandardCharsets.UTF_8));
        byte[] storedKey =
                MessageDigest.getInstance(algorithms.digest()).digest(clientKey);
        byte[] clientSignature =
                hmac(
                        algorithms.hmac(),
                        storedKey,
                        authMessage.getBytes(StandardCharsets.UTF_8));
        byte[] proof = xor(clientKey, clientSignature);
        byte[] serverKey =
                hmac(algorithms.hmac(), salted, "Server Key".getBytes(StandardCharsets.UTF_8));
        byte[] serverSignature =
                hmac(
                        algorithms.hmac(),
                        serverKey,
                        authMessage.getBytes(StandardCharsets.UTF_8));
        return new ScramExchange(
                withoutProof + ",p=" + Base64.getEncoder().encodeToString(proof),
                serverSignature);
    }

    private static void verifyServerFinal(
            byte[] payload, byte[] expectedSignature) {
        Map<String, String> attributes =
                attributes(new String(payload, StandardCharsets.UTF_8));
        byte[] actual =
                Base64.getDecoder().decode(required(attributes, "v"));
        if (!MessageDigest.isEqual(actual, expectedSignature)) {
            throw new AssertionError("SCRAM server signature did not match");
        }
    }

    private static Map<String, String> attributes(String message) {
        Map<String, String> attributes = new HashMap<>();
        for (String part : message.split(",")) {
            String[] pair = part.split("=", 2);
            if (pair.length != 2 || attributes.put(pair[0], pair[1]) != null) {
                throw new AssertionError("malformed SCRAM attributes");
            }
        }
        return attributes;
    }

    private static String required(
            Map<String, String> attributes, String name) {
        String value = attributes.get(name);
        if (value == null) {
            throw new AssertionError("missing SCRAM attribute " + name);
        }
        return value;
    }

    private static byte[] hmac(String algorithm, byte[] key, byte[] input)
            throws Exception {
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(input);
    }

    private static byte[] xor(byte[] left, byte[] right) {
        byte[] output = new byte[left.length];
        for (int index = 0; index < output.length; index++) {
            output[index] = (byte) (left[index] ^ right[index]);
        }
        return output;
    }

    private static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    private static void writePacket(DataOutputStream output, byte[] payload)
            throws Exception {
        output.writeInt(payload.length);
        output.write(payload);
        output.flush();
    }

    private static byte[] readPacket(DataInputStream input) throws Exception {
        int size = input.readInt();
        if (size < 0 || size > 1024 * 1024) {
            throw new AssertionError("invalid packet size " + size);
        }
        byte[] payload = input.readNBytes(size);
        if (payload.length != size) {
            throw new AssertionError("truncated packet");
        }
        return payload;
    }

    private static void writeString(DataOutputStream output, String value)
            throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    private static String readString(ByteBuffer input) {
        int size = Short.toUnsignedInt(input.getShort());
        byte[] bytes = new byte[size];
        input.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static SSLContext sslContext(Path certificatePath)
            throws Exception {
        CertificateFactory certificates =
                CertificateFactory.getInstance("X.509");
        Certificate certificate =
                certificates.generateCertificate(
                        new ByteArrayInputStream(
                                Files.readAllBytes(certificatePath)));
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null);
        trustStore.setCertificateEntry("rutomq", certificate);
        TrustManagerFactory trustManagers =
                TrustManagerFactory.getInstance(
                        TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(trustStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagers.getTrustManagers(), new SecureRandom());
        return context;
    }

    private record ScramExchange(
            String clientFinal, byte[] expectedServerSignature) {}

    private record ScramAlgorithms(
            String pbkdf2, String hmac, String digest, int bits) {
        private static ScramAlgorithms forMechanism(String mechanism) {
            return switch (mechanism) {
                case "SCRAM-SHA-256" ->
                        new ScramAlgorithms(
                                "PBKDF2WithHmacSHA256",
                                "HmacSHA256",
                                "SHA-256",
                                256);
                case "SCRAM-SHA-512" ->
                        new ScramAlgorithms(
                                "PBKDF2WithHmacSHA512",
                                "HmacSHA512",
                                "SHA-512",
                                512);
                default ->
                        throw new IllegalArgumentException(
                                "unsupported mechanism " + mechanism);
            };
        }
    }
}
