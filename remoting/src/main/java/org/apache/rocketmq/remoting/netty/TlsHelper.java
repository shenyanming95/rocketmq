package org.apache.rocketmq.remoting.netty;

import io.netty.handler.ssl.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.Properties;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.*;

public class TlsHelper {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static DecryptionStrategy decryptionStrategy = new DecryptionStrategy() {
        @Override
        public InputStream decryptPrivateKey(final String privateKeyEncryptPath, final boolean forClient) throws IOException {
            return new FileInputStream(privateKeyEncryptPath);
        }
    };

    public static void registerDecryptionStrategy(final DecryptionStrategy decryptionStrategy) {
        TlsHelper.decryptionStrategy = decryptionStrategy;
    }

    public static SslContext buildSslContext(boolean forClient) throws IOException, CertificateException {
        File configFile = new File(TlsSystemConfig.tlsConfigFile);
        extractTlsConfigFromFile(configFile);
        logTheFinalUsedTlsConfig();

        SslProvider provider;
        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
            LOGGER.info("Using OpenSSL provider");
        } else {
            provider = SslProvider.JDK;
            LOGGER.info("Using JDK SSL provider");
        }

        if (forClient) {
            if (tlsTestModeEnable) {
                return SslContextBuilder.forClient().sslProvider(SslProvider.JDK).trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } else {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);


                if (!tlsClientAuthServer) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (!isNullOrEmpty(tlsClientTrustCertPath)) {
                        sslContextBuilder.trustManager(new File(tlsClientTrustCertPath));
                    }
                }

                return sslContextBuilder.keyManager(!isNullOrEmpty(tlsClientCertPath) ? new FileInputStream(tlsClientCertPath) : null, !isNullOrEmpty(tlsClientKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsClientKeyPath, true) : null, !isNullOrEmpty(tlsClientKeyPassword) ? tlsClientKeyPassword : null).build();
            }
        } else {

            if (tlsTestModeEnable) {
                SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
                return SslContextBuilder.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey()).sslProvider(SslProvider.JDK).clientAuth(ClientAuth.OPTIONAL).build();
            } else {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(!isNullOrEmpty(tlsServerCertPath) ? new FileInputStream(tlsServerCertPath) : null, !isNullOrEmpty(tlsServerKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsServerKeyPath, false) : null, !isNullOrEmpty(tlsServerKeyPassword) ? tlsServerKeyPassword : null).sslProvider(provider);

                if (!tlsServerAuthClient) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (!isNullOrEmpty(tlsServerTrustCertPath)) {
                        sslContextBuilder.trustManager(new File(tlsServerTrustCertPath));
                    }
                }

                sslContextBuilder.clientAuth(parseClientAuthMode(tlsServerNeedClientAuth));
                return sslContextBuilder.build();
            }
        }
    }

    private static void extractTlsConfigFromFile(final File configFile) {
        if (!(configFile.exists() && configFile.isFile() && configFile.canRead())) {
            LOGGER.info("Tls config file doesn't exist, skip it");
            return;
        }

        Properties properties;
        properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(configFile);
            properties.load(inputStream);
        } catch (IOException ignore) {
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
            }
        }

        tlsTestModeEnable = Boolean.parseBoolean(properties.getProperty(TLS_TEST_MODE_ENABLE, String.valueOf(tlsTestModeEnable)));
        tlsServerNeedClientAuth = properties.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
        tlsServerKeyPath = properties.getProperty(TLS_SERVER_KEYPATH, tlsServerKeyPath);
        tlsServerKeyPassword = properties.getProperty(TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
        tlsServerCertPath = properties.getProperty(TLS_SERVER_CERTPATH, tlsServerCertPath);
        tlsServerAuthClient = Boolean.parseBoolean(properties.getProperty(TLS_SERVER_AUTHCLIENT, String.valueOf(tlsServerAuthClient)));
        tlsServerTrustCertPath = properties.getProperty(TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);

        tlsClientKeyPath = properties.getProperty(TLS_CLIENT_KEYPATH, tlsClientKeyPath);
        tlsClientKeyPassword = properties.getProperty(TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
        tlsClientCertPath = properties.getProperty(TLS_CLIENT_CERTPATH, tlsClientCertPath);
        tlsClientAuthServer = Boolean.parseBoolean(properties.getProperty(TLS_CLIENT_AUTHSERVER, String.valueOf(tlsClientAuthServer)));
        tlsClientTrustCertPath = properties.getProperty(TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
    }

    private static void logTheFinalUsedTlsConfig() {
        LOGGER.info("Log the final used tls related configuration");
        LOGGER.info("{} = {}", TLS_TEST_MODE_ENABLE, tlsTestModeEnable);
        LOGGER.info("{} = {}", TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
        LOGGER.info("{} = {}", TLS_SERVER_KEYPATH, tlsServerKeyPath);
        LOGGER.info("{} = {}", TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
        LOGGER.info("{} = {}", TLS_SERVER_CERTPATH, tlsServerCertPath);
        LOGGER.info("{} = {}", TLS_SERVER_AUTHCLIENT, tlsServerAuthClient);
        LOGGER.info("{} = {}", TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);

        LOGGER.info("{} = {}", TLS_CLIENT_KEYPATH, tlsClientKeyPath);
        LOGGER.info("{} = {}", TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
        LOGGER.info("{} = {}", TLS_CLIENT_CERTPATH, tlsClientCertPath);
        LOGGER.info("{} = {}", TLS_CLIENT_AUTHSERVER, tlsClientAuthServer);
        LOGGER.info("{} = {}", TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
    }

    private static ClientAuth parseClientAuthMode(String authMode) {
        if (null == authMode || authMode.trim().isEmpty()) {
            return ClientAuth.NONE;
        }

        for (ClientAuth clientAuth : ClientAuth.values()) {
            if (clientAuth.name().equals(authMode.toUpperCase())) {
                return clientAuth;
            }
        }

        return ClientAuth.NONE;
    }

    /**
     * Determine if a string is {@code null} or {@link String#isEmpty()} returns {@code true}.
     */
    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    public interface DecryptionStrategy {
        /**
         * Decrypt the target encrpted private key file.
         *
         * @param privateKeyEncryptPath A pathname string
         * @param forClient             tells whether it's a client-side key file
         * @return An input stream for a decrypted key file
         * @throws IOException if an I/O error has occurred
         */
        InputStream decryptPrivateKey(String privateKeyEncryptPath, boolean forClient) throws IOException;
    }
}
