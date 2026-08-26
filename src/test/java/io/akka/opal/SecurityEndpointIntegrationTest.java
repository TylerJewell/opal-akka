package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * The secured server, over real HTTP — SPEC-002 R71 to R74, compared with the refusals the
 * original produced when it was run with the same keys and master token.
 */
public class SecurityEndpointIntegrationTest extends TestKitSupport {

  private static final Path JWKS_DIR;

  static {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      KeyPair pair = generator.generateKeyPair();
      JWKS_DIR = Files.createTempDirectory("opal-jwks-");
      System.setProperty("OPAL_ROLE", "server");
      System.setProperty("OPAL_AUTH_PRIVATE_KEY", pem("PRIVATE KEY", pair.getPrivate().getEncoded()));
      System.setProperty("OPAL_AUTH_PRIVATE_KEY_FORMAT", "pem");
      System.setProperty("OPAL_AUTH_PUBLIC_KEY", pem("PUBLIC KEY", pair.getPublic().getEncoded()));
      System.setProperty("OPAL_AUTH_PUBLIC_KEY_FORMAT", "pem");
      System.setProperty("OPAL_AUTH_MASTER_TOKEN", "master-secret");
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
        java.util.List.of(
            "OPAL_ROLE",
            "OPAL_AUTH_PRIVATE_KEY",
            "OPAL_AUTH_PRIVATE_KEY_FORMAT",
            "OPAL_AUTH_PUBLIC_KEY",
            "OPAL_AUTH_PUBLIC_KEY_FORMAT",
            "OPAL_AUTH_MASTER_TOKEN",
            "OPAL_AUTH_JWKS_STATIC_DIR")) {
      System.clearProperty(name);
    }
  }

  private static JsonNode recorded(String key) {
    return SourceAnswers.LIVE_SECURE.get(key);
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  private String mintToken(String type, String claimsJson) {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/token")
            .addHeader("Authorization", "bearer master-secret")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                ("{\"type\": \"" + type + "\", \"claims\": " + claimsJson + "}")
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(200, response.status().intValue());
    return body(response).get("token").asText();
  }

  /** R72: the master token is required, and a wrong one is refused with the source's words. */
  @Test
  public void tokenMintingRequiresTheMasterToken() {
    StrictResponse<ByteString> none =
        httpClient
            .POST("/token")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"type\": \"client\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("token_no_master").get("status").asInt(), none.status().intValue());
    assertEquals(
        recorded("token_no_master").get("body").get("detail").get("error").asText(),
        body(none).get("detail").get("error").asText());

    StrictResponse<ByteString> wrong =
        httpClient
            .POST("/token")
            .addHeader("Authorization", "bearer nope")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"type\": \"client\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("token_wrong_master").get("status").asInt(), wrong.status().intValue());
    assertEquals(
        recorded("token_wrong_master").get("body").get("detail").get("error").asText(),
        body(wrong).get("detail").get("error").asText());
  }

  /** R68: a minted token carries its peer type, and the details say what was minted. */
  @Test
  public void aMintedTokenMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/token")
            .addHeader("Authorization", "bearer master-secret")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"type\": \"client\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("token_client").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("token_client").get("type").asText(), body(response).get("type").asText());
    assertEquals(
        recorded("token_client").get("details_type").asText(),
        body(response).get("details").get("type").asText());
    assertEquals(
        recorded("token_client").get("claims").get("peer_type").asText(),
        body(response).get("details").get("claims").get("peer_type").asText());

    JsonNode datasource =
        Rpc.MAPPER.valueToTree(recorded("token_datasource_claims"));
    String token =
        mintToken(
            "datasource",
            "{\"permitted_topics\": [\"policy_data\"]}");
    assertTrue(token.split("\\.").length == 3);
    assertEquals("datasource", datasource.get("peer_type").asText());
  }

  /** R71: a missing token and an unreadable one are two different refusals. */
  @Test
  public void refusalsMatchTheSource() {
    StrictResponse<ByteString> none = httpClient.GET("/policy").invoke();
    assertEquals(recorded("policy_no_token").get("status").asInt(), none.status().intValue());
    assertEquals(
        recorded("policy_no_token").get("body").get("detail").get("error").asText(),
        body(none).get("detail").get("error").asText());
    assertTrue(
        none.httpResponse().getHeader("WWW-Authenticate").isPresent(),
        "R71: the challenge header is part of the refusal");

    StrictResponse<ByteString> bad =
        httpClient.GET("/policy").addHeader("Authorization", "bearer not.a.jwt").invoke();
    assertEquals(recorded("policy_bad_token").get("status").asInt(), bad.status().intValue());
    assertEquals(
        recorded("policy_bad_token").get("body").get("detail").get("error").asText(),
        body(bad).get("detail").get("error").asText());
    assertEquals("not.a.jwt", body(bad).get("detail").get("token").asText());
  }

  /** R45 and R73: publishing needs a datasource token, and a client token is the wrong one. */
  @Test
  public void publishingChecksThePeerType() {
    String clientToken = mintToken("client", "{}");
    StrictResponse<ByteString> asClient =
        httpClient
            .POST("/data/config")
            .addHeader("Authorization", "bearer " + clientToken)
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"entries\": [], \"reason\": \"probe\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("publish_as_client").get("status").asInt(), asClient.status().intValue());
    assertEquals(
        recorded("publish_as_client").get("body").get("detail").get("error").asText(),
        body(asClient).get("detail").get("error").asText());

    String datasourceToken = mintToken("datasource", "{}");
    StrictResponse<ByteString> asDatasource =
        httpClient
            .POST("/data/config")
            .addHeader("Authorization", "bearer " + datasourceToken)
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"entries\": [], \"reason\": \"probe\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(
        recorded("publish_as_datasource").get("status").asInt(), asDatasource.status().intValue());
    assertEquals(recorded("publish_as_datasource").get("body"), body(asDatasource));
  }

  /** R45: a token carrying permitted topics may not publish outside them — nor on a sub-topic. */
  @Test
  public void permittedTopicsAreEnforcedIncludingSubTopics() {
    String restricted = mintToken("datasource", "{\"permitted_topics\": [\"policy_data\"]}");

    StrictResponse<ByteString> forbidden =
        httpClient
            .POST("/data/config")
            .addHeader("Authorization", "bearer " + restricted)
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                ("{\"entries\": [{\"url\": \"http://x\", \"topics\": [\"other\"]}],"
                        + " \"reason\": \"probe\"}")
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(
        recorded("publish_forbidden_topic").get("status").asInt(), forbidden.status().intValue());
    assertEquals(
        recorded("publish_forbidden_topic").get("body").get("detail").get("error").asText(),
        body(forbidden).get("detail").get("error").asText());

    StrictResponse<ByteString> subTopic =
        httpClient
            .POST("/data/config")
            .addHeader("Authorization", "bearer " + restricted)
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                ("{\"entries\": [{\"url\": \"http://x\", \"topics\": [\"policy_data/users\"]}],"
                        + " \"reason\": \"probe\"}")
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(
        recorded("publish_subtopic_under_restricted_token").get("status").asInt(),
        subTopic.status().intValue());
    assertEquals(
        recorded("publish_subtopic_under_restricted_token")
            .get("body").get("detail").get("error").asText(),
        body(subTopic).get("detail").get("error").asText());
  }

  /** R74: with a key configured the JWKS document carries it. */
  @Test
  public void theJwksDocumentCarriesTheKey() {
    StrictResponse<ByteString> response = httpClient.GET("/.well-known/jwks.json").invoke();
    assertEquals(recorded("jwks").get("status").asInt(), response.status().intValue());
    JsonNode keys = body(response).get("keys");
    assertEquals(1, keys.size());
    assertEquals(
        recorded("jwks").get("body").get("keys").get(0).get("kty").asText(),
        keys.get(0).get("kty").asText());
    assertEquals(
        recorded("jwks").get("body").get("keys").get(0).get("e").asText(),
        keys.get(0).get("e").asText());
  }
}
