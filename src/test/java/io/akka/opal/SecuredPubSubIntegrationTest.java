package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.testkit.TestKitSupport;
import akka.javasdk.testkit.WebSocketRouteTester;
import io.akka.opal.server.pubsub.Rpc;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Map;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R75 — what a websocket without a token gets from a server that requires one.
 *
 * <p>The authenticator answers with no claims rather than raising, and the endpoint refuses the
 * connection. That is not a stylistic choice: a websocket handshake has nowhere to put a 401
 * body, so refusing the upgrade is the only answer available, and a caller distinguishes it from
 * a working connection by there being no connection.
 *
 * <p>The other half of the pair is {@link PubSubEndpointIntegrationTest}, which runs the same
 * endpoint with no keys configured and gets an answer to every call. A configuration is read once
 * per process here as it is in the original, so the two halves cannot live in one class.
 */
public class SecuredPubSubIntegrationTest extends TestKitSupport {

  private static final Path JWKS_DIR;

  static {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      KeyPair pair = generator.generateKeyPair();
      JWKS_DIR = Files.createTempDirectory("opal-jwks-ws-");
      System.setProperty("OPAL_ROLE", "server");
      System.setProperty(
          "OPAL_AUTH_PRIVATE_KEY", pem("PRIVATE KEY", pair.getPrivate().getEncoded()));
      System.setProperty("OPAL_AUTH_PRIVATE_KEY_FORMAT", "pem");
      System.setProperty("OPAL_AUTH_PUBLIC_KEY", pem("PUBLIC KEY", pair.getPublic().getEncoded()));
      System.setProperty("OPAL_AUTH_PUBLIC_KEY_FORMAT", "pem");
      System.setProperty("OPAL_AUTH_JWKS_STATIC_DIR", JWKS_DIR.toString());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String pem(String type, byte[] der) throws Exception {
    StringWriter out = new StringWriter();
    try (PemWriter writer = new PemWriter(out)) {
      writer.writeObject(new PemObject(type, der));
    }
    return out.toString();
  }

  @AfterAll
  public static void clearProperties() {
    for (String name :
        List.of(
            "OPAL_ROLE",
            "OPAL_AUTH_PRIVATE_KEY",
            "OPAL_AUTH_PRIVATE_KEY_FORMAT",
            "OPAL_AUTH_PUBLIC_KEY",
            "OPAL_AUTH_PUBLIC_KEY_FORMAT",
            "OPAL_AUTH_JWKS_STATIC_DIR")) {
      System.clearProperty(name);
    }
  }

  private static final String UPGRADE_REFUSED = "upgrade refused";
  private static final String STREAM_ENDED = "stream ended with no answer";

  /**
   * R75: with verification on and no token, the socket does not carry a conversation.
   *
   * <p>Two outcomes are correct and which one appears is the runtime's business — it may refuse
   * the upgrade, or accept it and end the stream. What must not happen is an answer, and both
   * branches below fail if one arrives: {@code expectSubscriptionAndComplete} fails on an
   * element, and the other branch is only reached when no connection was made at all.
   */
  @Test
  public void anUnauthenticatedSocketIsRefused() {
    String outcome;
    try {
      WebSocketRouteTester.WsConnection<String> connection =
          testKit.getSelfWebSocketRouteTester().wsTextConnection("/ws");
      connection
          .publisher()
          .sendNext(
              Rpc.serialize(
                  Rpc.RpcMessage.request(
                      "subscribe", Map.of("topics", List.of("policy_data")), "c1")));
      connection.subscriber().request(1);
      connection.subscriber().expectSubscriptionAndComplete();
      outcome = STREAM_ENDED;
    } catch (AssertionError failed) {
      throw failed;
    } catch (Exception e) {
      // A refused upgrade and an unreachable service look the same from here, so the service
      // is asked a question it will answer before this is read as a refusal.
      assertEquals(
          200,
          httpClient.GET("/").invoke().status().intValue(),
          "the service is up, so the socket is what was refused");
      outcome = UPGRADE_REFUSED;
    }
    assertTrue(
        List.of(STREAM_ENDED, UPGRADE_REFUSED).contains(outcome), "outcome was " + outcome);
  }

  /** R74: the JWKS document is served whether or not the caller has a token. */
  @Test
  public void theJwksIsPublicOnASecuredServer() {
    var response = httpClient.GET("/.well-known/jwks.json").invoke();
    assertTrue(response.status().intValue() == 200, "status " + response.status());
    assertTrue(
        response.body().decodeString(java.nio.charset.StandardCharsets.UTF_8).contains("keys"),
        "the document names its keys");
  }
}
