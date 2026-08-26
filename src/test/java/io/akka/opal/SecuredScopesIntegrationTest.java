package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import akka.http.javadsl.model.ContentTypes;
import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.server.pubsub.Rpc;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R110 — the claim the two scoped read routes need, and the ones they do not.
 *
 * <p>This is the multi-tenant boundary. A bundle is one tenant's policy, and the claim naming
 * which scopes a token may read is the whole of what keeps one tenant from loading another's into
 * its own engine. The three cases below are the ones that matter and they answer differently: a
 * token naming this scope is served, one naming a different scope is refused, and — the one a
 * rebuild gets wrong — a token naming no scopes at all is refused rather than admitted.
 *
 * <p>The CRUD routes are here too, with the same claimless token, because the rule is that they
 * do <em>not</em> need it: a check that refused everywhere would pass the three cases above and
 * be wrong.
 */
public class SecuredScopesIntegrationTest extends TestKitSupport {

  private static final Path JWKS_DIR;

  static {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      KeyPair pair = generator.generateKeyPair();
      JWKS_DIR = Files.createTempDirectory("opal-jwks-scopes-");
      System.setProperty("OPAL_ROLE", "server");
      System.setProperty("OPAL_SCOPES", "true");
      System.setProperty(
          "OPAL_AUTH_PRIVATE_KEY", pem("PRIVATE KEY", pair.getPrivate().getEncoded()));
      System.setProperty("OPAL_AUTH_PRIVATE_KEY_FORMAT", "pem");
      System.setProperty("OPAL_AUTH_PUBLIC_KEY", pem("PUBLIC KEY", pair.getPublic().getEncoded()));
      System.setProperty("OPAL_AUTH_PUBLIC_KEY_FORMAT", "pem");
      System.setProperty("OPAL_AUTH_MASTER_TOKEN", "master-secret");
      System.setProperty("OPAL_AUTH_JWKS_STATIC_DIR", JWKS_DIR.toString());
      System.setProperty(
          "OPAL_BASE_DIR", Files.createTempDirectory("opal-scopes-base-").toString());
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
            "OPAL_SCOPES",
            "OPAL_AUTH_PRIVATE_KEY",
            "OPAL_AUTH_PRIVATE_KEY_FORMAT",
            "OPAL_AUTH_PUBLIC_KEY",
            "OPAL_AUTH_PUBLIC_KEY_FORMAT",
            "OPAL_AUTH_MASTER_TOKEN",
            "OPAL_AUTH_JWKS_STATIC_DIR",
            "OPAL_BASE_DIR")) {
      System.clearProperty(name);
    }
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  private String mint(String claimsJson) {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/token")
            .addHeader("Authorization", "bearer master-secret")
            .withRequestBody(
                ContentTypes.APPLICATION_JSON,
                ("{\"type\": \"datasource\", \"claims\": " + claimsJson + "}")
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(200, response.status().intValue());
    return body(response).get("token").asText();
  }

  private int get(String path, String token) {
    return httpClient
        .GET(path)
        .addHeader("Authorization", "bearer " + token)
        .invoke()
        .status()
        .intValue();
  }

  /** R110: the claim decides, and its absence decides against. */
  @Test
  public void theBundleAndDataRoutesNeedTheScopeInTheClaim() {
    String scoped = mint("{\"allowed_scopes\": [\"alpha\"]}");
    String otherScope = mint("{\"allowed_scopes\": [\"beta\"]}");
    String claimless = mint("{}");

    // A token naming another scope, and one naming none, are both refused. The two are separate
    // cases: reading a missing claim as "unrestricted" passes the first and fails the second.
    assertEquals(403, get("/scopes/alpha/policy", otherScope), "a token for another tenant");
    assertEquals(403, get("/scopes/alpha/policy", claimless), "a token naming no scopes");
    assertEquals(403, get("/scopes/alpha/data", otherScope));
    assertEquals(403, get("/scopes/alpha/data", claimless));

    // The token that names this scope gets past the claim check. What it meets next is the
    // scope not existing, which is a different answer and the point: 403 is the claim, and
    // anything else means the claim let it through.
    assertNotEquals(403, get("/scopes/alpha/policy", scoped), "the claim admitted it");
  }

  /** R110's other half: the CRUD routes take a `datasource` token and no scope claim. */
  @Test
  public void theCrudRoutesDoNotNeedTheScopeClaim() {
    String claimless = mint("{}");
    assertEquals(200, get("/scopes", claimless), "listing needs no scope claim");
    assertEquals(404, get("/scopes/alpha", claimless), "and neither does reading one");
  }

  /** A token of the wrong peer type is refused before the scope claim is looked at. */
  @Test
  public void aClientTokenIsRefusedOnTheScopeRoutes() {
    StrictResponse<ByteString> minted =
        httpClient
            .POST("/token")
            .addHeader("Authorization", "bearer master-secret")
            .withRequestBody(
                ContentTypes.APPLICATION_JSON,
                "{\"type\": \"client\", \"claims\": {\"allowed_scopes\": [\"alpha\"]}}"
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    String clientToken = body(minted).get("token").asText();
    assertEquals(401, get("/scopes", clientToken));
  }
}
