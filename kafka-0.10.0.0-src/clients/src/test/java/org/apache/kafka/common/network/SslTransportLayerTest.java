/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Map;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.config.types.Password;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the SSL transport layer. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SslTransportLayerTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private NioEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private CertStores serverCertStores;
    private CertStores clientCertStores;
    private Map<String, Object> sslClientConfigs;
    private Map<String, Object> sslServerConfigs;

    @Before
    public void setup() throws Exception {
        // Create certificates for use by client and server. Add server cert to client truststore and vice versa.
        serverCertStores = new CertStores(true, "localhost");
        clientCertStores = new CertStores(false, "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
    }

    @After
    public void teardown() throws Exception {
        if (selector != null)
            this.selector.close();
        if (server != null)
            this.server.close();
    }

    /**
     * Tests that server certificate with valid IP address is accepted by
     * a client that validates server endpoint.
     */
    @Test
    public void testValidEndpointIdentification() throws Exception {
        String node = "0";
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server certificate with invalid host name is not accepted by
     * a client that validates server endpoint. Server certificate uses
     * wrong hostname as common name to trigger endpoint validation failure.
     */
    @Test
    public void testInvalidEndpointIdentification() throws Exception {
        String node = "0";
        serverCertStores = new CertStores(true, "notahost");
        clientCertStores = new CertStores(false, "localhost");
        sslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        sslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
        sslClientConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }
    
    /**
     * Tests that server certificate with invalid IP address is accepted by
     * a client that has disabled endpoint validation
     */
    @Test
    public void testEndpointIdentificationDisabled() throws Exception {
        String node = "0";
        String serverHost = InetAddress.getLocalHost().getHostAddress();
        server = new NioEchoServer(SecurityProtocol.SSL, sslServerConfigs, serverHost);
        server.start();
        sslClientConfigs.remove(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress(serverHost, server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from clients with a trusted certificate
     * when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredValidProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server does not accept connections from clients with an untrusted certificate
     * when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredUntrustedProvided() throws Exception {
        String node = "0";
        sslServerConfigs = serverCertStores.getUntrustingConfig();
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }
    
    /**
     * Tests that server does not accept connections from clients which don't
     * provide a certificate when client authentication is required.
     */
    @Test
    public void testClientAuthenticationRequiredNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }
    
    /**
     * Tests that server accepts connections from a client configured
     * with an untrusted certificate if client authentication is disabled
     */
    @Test
    public void testClientAuthenticationDisabledUntrustedProvided() throws Exception {
        String node = "0";
        sslServerConfigs = serverCertStores.getUntrustingConfig();
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is disabled
     */
    @Test
    public void testClientAuthenticationDisabledNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "none");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client configured
     * with a valid certificate if client authentication is requested
     */
    @Test
    public void testClientAuthenticationRequestedValidProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that server accepts connections from a client that does not provide
     * a certificate if client authentication is requested but not required
     */
    @Test
    public void testClientAuthenticationRequestedNotProvided() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "requested");
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslClientConfigs.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    /**
     * Tests that channels cannot be created if truststore cannot be loaded
     */
    @Test
    public void testInvalidTruststorePassword() throws Exception {
        SslChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        try {
            sslClientConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "invalid");
            channelBuilder.configure(sslClientConfigs);
            fail("SSL channel configured with invalid truststore password");
        } catch (KafkaException e) {
            // Expected exception
        }
    }
    
    /**
     * Tests that channels cannot be created if keystore cannot be loaded
     */
    @Test
    public void testInvalidKeystorePassword() throws Exception {
        SslChannelBuilder channelBuilder = new SslChannelBuilder(Mode.CLIENT);
        try {
            sslClientConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "invalid");
            channelBuilder.configure(sslClientConfigs);
            fail("SSL channel configured with invalid keystore password");
        } catch (KafkaException e) {
            // Expected exception
        }
    }
    
    /**
     * Tests that client connections cannot be created to a server
     * if key password is invalid
     */
    @Test
    public void testInvalidKeyPassword() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, new Password("invalid"));
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }
    
    /**
     * Tests that connections cannot be made with unsupported TLS versions
     */
    @Test
    public void testUnsupportedTLSVersion() throws Exception {
        String node = "0";
        sslServerConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.2"));
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        
        sslClientConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.1"));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }
    
    /**
     * Tests that connections cannot be made with unsupported TLS cipher suites
     */
    @Test
    public void testUnsupportedCiphers() throws Exception {
        String node = "0";
        String[] cipherSuites = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites();
        sslServerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[0]));
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        
        sslClientConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Arrays.asList(cipherSuites[1]));
        createSelector(sslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }

    /**
     * Tests handling of BUFFER_UNDERFLOW during unwrap when network read buffer is smaller than SSL session packet buffer size.
     */
    @Test
    public void testNetReadBufferResize() throws Exception {
        String node = "0";
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs, 10, null, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }
    
    /**
     * Tests handling of BUFFER_OVERFLOW during wrap when network write buffer is smaller than SSL session packet buffer size.
     */
    @Test
    public void testNetWriteBufferResize() throws Exception {
        String node = "0";
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs, null, 10, null);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }

    /**
     * Tests handling of BUFFER_OVERFLOW during unwrap when application read buffer is smaller than SSL session application buffer size.
     */
    @Test
    public void testApplicationBufferResize() throws Exception {
        String node = "0";
        server = NetworkTestUtils.createEchoServer(SecurityProtocol.SSL, sslServerConfigs);
        createSelector(sslClientConfigs, null, null, 10);
        InetSocketAddress addr = new InetSocketAddress("localhost", server.port());
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 64000, 10);
    }
    
    private void createSelector(Map<String, Object> sslClientConfigs) {
        createSelector(sslClientConfigs, null, null, null);
    }      

    private void createSelector(Map<String, Object> sslClientConfigs, final Integer netReadBufSize, final Integer netWriteBufSize, final Integer appBufSize) {
        
        this.channelBuilder = new SslChannelBuilder(Mode.CLIENT) {

            @Override
            protected SslTransportLayer buildTransportLayer(SslFactory sslFactory, String id, SelectionKey key) throws IOException {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                SSLEngine sslEngine = sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(),
                                socketChannel.socket().getPort());
                TestSslTransportLayer transportLayer = new TestSslTransportLayer(id, key, sslEngine, netReadBufSize, netWriteBufSize, appBufSize);
                transportLayer.startHandshake();
                return transportLayer;
            }


        };
        this.channelBuilder.configure(sslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", channelBuilder);
    }
    
    /**
     * SSLTransportLayer with overrides for packet and application buffer size to test buffer resize
     * code path. The overridden buffer size starts with a small value and increases in size when the buffer
     * size is retrieved to handle overflow/underflow, until the actual session buffer size is reached.
     */
    private static class TestSslTransportLayer extends SslTransportLayer {

        private final ResizeableBufferSize netReadBufSize;
        private final ResizeableBufferSize netWriteBufSize;
        private final ResizeableBufferSize appBufSize;

        public TestSslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine,
                                     Integer netReadBufSize, Integer netWriteBufSize, Integer appBufSize) throws IOException {
            super(channelId, key, sslEngine, false);
            this.netReadBufSize = new ResizeableBufferSize(netReadBufSize);
            this.netWriteBufSize = new ResizeableBufferSize(netWriteBufSize);
            this.appBufSize = new ResizeableBufferSize(appBufSize);
        }
        
        @Override
        protected int netReadBufferSize() {
            ByteBuffer netReadBuffer = netReadBuffer();
            // netReadBufferSize() is invoked in SSLTransportLayer.read() prior to the read
            // operation. To avoid the read buffer being expanded too early, increase buffer size
            // only when read buffer is full. This ensures that BUFFER_UNDERFLOW is always
            // triggered in testNetReadBufferResize().
            boolean updateBufSize = netReadBuffer != null && !netReadBuffer().hasRemaining();
            return netReadBufSize.updateAndGet(super.netReadBufferSize(), updateBufSize);
        }
        
        @Override
        protected int netWriteBufferSize() {
            return netWriteBufSize.updateAndGet(super.netWriteBufferSize(), true);
        }

        @Override
        protected int applicationBufferSize() {
            return appBufSize.updateAndGet(super.applicationBufferSize(), true);
        }
        
        private static class ResizeableBufferSize {
            private Integer bufSizeOverride;
            ResizeableBufferSize(Integer bufSizeOverride) {
                this.bufSizeOverride = bufSizeOverride;
            }
            int updateAndGet(int actualSize, boolean update) {
                int size = actualSize;
                if (bufSizeOverride != null) {
                    if (update)
                        bufSizeOverride = Math.min(bufSizeOverride * 2, size);
                    size = bufSizeOverride;
                }
                return size;
            }
        }
    }

}
