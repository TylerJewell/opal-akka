package io.akka.opal.common.util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

/**
 * The HTTP clients OPAL builds: a plain one, one that trusts a named CA file, and one that
 * presents a client certificate. All three come from the same place so a configuration change
 * cannot leave one call path using different trust from another.
 */
public final class Http {

  static {
    if (java.security.Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      java.security.Security.addProvider(new BouncyCastleProvider());
    }
  }

  /**
   * R291: the egress proxy the environment names, applied to every outbound call.
   *
   * <p>Installed only when something is configured, so a process with no proxy variables keeps
   * the direct connection rather than going through a selector that always answers "no proxy".
   */
  private static final EnvironmentProxySelector PROXIES = new EnvironmentProxySelector();

  private static final HttpClient PLAIN = withProxy(HttpClient.newBuilder()).build();

  static HttpClient.Builder withProxy(HttpClient.Builder builder) {
    builder.connectTimeout(Duration.ofSeconds(10));
    if (PROXIES.configured()) {
      builder.proxy(PROXIES);
    }
    return builder;
  }

  private static volatile HttpClient CLIENT = PLAIN;

  private Http() {}

  public static HttpClient plain() {
    return PLAIN;
  }

  /**
   * R384: one outbound call, inside a span of its own.
   *
   * <p>The source's tracer instruments the libraries it uses, so a trace has a child span per
   * network call and the parent's own time is readable against them. Nothing instruments a call
   * here on its own, so the calls this rebuild makes open their spans themselves — which covers
   * the same set, because every outbound call OPAL makes goes through one of the three clients
   * this class hands out.
   */
  public static <T> java.net.http.HttpResponse<T> send(
      HttpClient client,
      java.net.http.HttpRequest request,
      java.net.http.HttpResponse.BodyHandler<T> handler)
      throws java.io.IOException, InterruptedException {
    try (io.akka.opal.common.monitoring.Span span =
        io.akka.opal.common.monitoring.Apm.trace(
            "http.request", request.method() + " " + Urls.redactUrl(request.uri().toString()))) {
      java.net.http.HttpResponse<T> response = client.send(request, handler);
      if (span != null) {
        span.setTag("http.method", request.method());
        span.setTag("http.url", Urls.redactUrl(request.uri().toString()));
        span.setTag("http.status_code", String.valueOf(response.statusCode()));
        span.setTag("span.kind", "client");
      }
      return response;
    }
  }

  /**
   * R184: the client every server-facing call uses, which may trust one extra certificate
   * authority.
   *
   * <p>The source reads the two entries at each of its six construction sites; read once here
   * instead, so no call path can end up with different trust from another.
   */
  public static HttpClient forClient() {
    return CLIENT;
  }

  /**
   * R184: both entries have to say yes.
   *
   * <p>Allowing self-signed certificates on its own trusts nothing extra, and naming a file on
   * its own is ignored. The file must also be there — a path that does not resolve leaves the
   * default trust in place rather than failing every call, which is the source's answer.
   */
  public static void configureClientTrust(boolean selfSignedAllowed, String trustedCaFile) {
    CLIENT = clientFor(selfSignedAllowed, trustedCaFile);
  }

  public static HttpClient clientFor(boolean selfSignedAllowed, String trustedCaFile) {
    if (!selfSignedAllowed || trustedCaFile == null || trustedCaFile.isBlank()) {
      return PLAIN;
    }
    Path resolved = expandUser(trustedCaFile);
    if (!Files.isRegularFile(resolved)) {
      return PLAIN;
    }
    return withTls(resolved.toString(), null, null);
  }

  /** A leading {@code ~} means the running user's home, the way the source's own reader has it. */
  static Path expandUser(String path) {
    if (path.startsWith("~")) {
      return Path.of(System.getProperty("user.home"), path.substring(1));
    }
    return Path.of(path);
  }

  /**
   * A client that trusts {@code caFile} and — when both are given — presents the client
   * certificate. Passing all three nulls returns the plain client rather than a permissive one:
   * silently trusting everything is the failure mode this signature exists to avoid.
   */
  public static HttpClient withTls(String caFile, String clientCert, String clientKey) {
    if (caFile == null && clientCert == null && clientKey == null) {
      return PLAIN;
    }
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      TrustManager[] trustManagers = caFile == null ? null : trustManagersFor(caFile);
      KeyManagerFactory keyManagers =
          clientCert != null && clientKey != null ? keyManagersFor(clientCert, clientKey) : null;
      context.init(
          keyManagers == null ? null : keyManagers.getKeyManagers(),
          trustManagers,
          new SecureRandom());
      return withProxy(HttpClient.newBuilder().sslContext(context)).build();
    } catch (Exception e) {
      throw new IllegalArgumentException("could not build a TLS context: " + e.getMessage(), e);
    }
  }

  private static TrustManager[] trustManagersFor(String caFile) throws Exception {
    KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
    store.load(null, null);
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    int index = 0;
    try (InputStream in = new FileInputStream(caFile)) {
      for (java.security.cert.Certificate certificate : factory.generateCertificates(in)) {
        store.setCertificateEntry("ca" + index++, certificate);
      }
    }
    TrustManagerFactory trust =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trust.init(store);
    return trust.getTrustManagers();
  }

  private static KeyManagerFactory keyManagersFor(String certFile, String keyFile)
      throws Exception {
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    List<java.security.cert.Certificate> chain = new ArrayList<>();
    try (InputStream in = new FileInputStream(certFile)) {
      chain.addAll(factory.generateCertificates(in));
    }
    PrivateKey key = readPrivateKey(Files.readString(Path.of(keyFile), StandardCharsets.UTF_8));
    KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
    store.load(null, null);
    store.setKeyEntry("client", key, new char[0], chain.toArray(new java.security.cert.Certificate[0]));
    KeyManagerFactory keys =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keys.init(store, new char[0]);
    return keys;
  }

  private static PrivateKey readPrivateKey(String pem) throws Exception {
    try (PEMParser parser = new PEMParser(new java.io.StringReader(pem))) {
      Object object = parser.readObject();
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
      if (object instanceof PEMKeyPair pair) {
        return converter.getKeyPair(pair).getPrivate();
      }
      if (object instanceof org.bouncycastle.asn1.pkcs.PrivateKeyInfo info) {
        return converter.getPrivateKey(info);
      }
      throw new IllegalArgumentException("not a private key");
    }
  }

  public static String basicAuth(String username, String password) {
    String raw = username + ":" + password;
    return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  /** Reads an X.509 certificate out of PEM text, for the places OPAL takes one inline. */
  public static X509Certificate certificate(String pem) throws Exception {
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    return (X509Certificate)
        factory.generateCertificate(new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
  }
}
