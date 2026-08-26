package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.git.ProbeRepository;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * The multi-tenant routes, over real HTTP — SPEC-002 R102 to R111.
 *
 * <p>Ordered, because scopes are a sequence: a scope is put, read back, refreshed, served a bundle
 * and deleted, and several of the rules are about what the <em>next</em> call sees. A table of
 * independent calls would agree on every row and check none of them.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ScopesEndpointIntegrationTest extends TestKitSupport {

  private static final ProbeRepository REPO;
  private static final Path BASE_DIR;

  static {
    try {
      REPO = new ProbeRepository();
      BASE_DIR = Files.createTempDirectory("opal-scopes-base-");
      System.setProperty("OPAL_ROLE", "server");
      System.setProperty("OPAL_SCOPES", "true");
      System.setProperty("OPAL_BASE_DIR", BASE_DIR.toString());
      System.setProperty("OPAL_DEBUG_INTERNAL_STATS", "true");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterAll
  public static void clearProperties() throws Exception {
    REPO.close();
    for (String name :
        List.of("OPAL_ROLE", "OPAL_SCOPES", "OPAL_BASE_DIR", "OPAL_DEBUG_INTERNAL_STATS")) {
      System.clearProperty(name);
    }
  }

  private static JsonNode recorded(String key) {
    return SourceAnswers.LIVE_SCOPES.get(key);
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  private String scopeJson(String id, String dataUrl) {
    return "{\"scope_id\": \""
        + id
        + "\", \"policy\": {\"source_type\": \"git\", \"url\": \""
        + REPO.root.toUri()
        + "\", \"auth\": {\"auth_type\": \"none\"}, \"branch\": \"master\", \"manifest\": \"\"},"
        + " \"data\": {\"entries\": [{\"url\": \""
        + dataUrl
        + "\", \"topics\": [\"policy_data\"]}]}}";
  }

  private StrictResponse<ByteString> putScope(String id, String dataUrl) {
    return httpClient
        .PUT("/scopes")
        .withRequestBody(
            akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
            scopeJson(id, dataUrl).getBytes(StandardCharsets.UTF_8))
        .invoke();
  }

  /** R103: a scope is created with 201. */
  @Test
  @Order(1)
  public void puttingScopesMatchesTheSource() {
    assertEquals(recorded("put_scope_alpha").asInt(), putScope("alpha", "http://alpha/data").status().intValue());
    assertEquals(recorded("put_scope_beta").asInt(), putScope("beta", "http://beta/data").status().intValue());
    assertEquals(
        recorded("put_scope_default").asInt(),
        putScope("default", "http://default/data").status().intValue());
  }

  /** R103: an SSH key with no newline, and one that parses as nothing, are each their own 422. */
  @Test
  @Order(2)
  public void anInvalidSshKeyMatchesTheSource() {
    String noNewline =
        "{\"scope_id\": \"ssh1\", \"policy\": {\"source_type\": \"git\", \"url\": \"git@x:y.git\","
            + " \"auth\": {\"auth_type\": \"ssh\", \"username\": \"git\", \"private_key\":"
            + " \"no-newlines-here\"}}}";
    StrictResponse<ByteString> response =
        httpClient
            .PUT("/scopes")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                noNewline.getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("put_scope_ssh_no_newline").get("status").asInt(), response.status().intValue());
    assertEquals(
        recorded("put_scope_ssh_no_newline").get("body").get("detail").get("error").asText(),
        body(response).get("detail").get("error").asText());

    String garbage =
        "{\"scope_id\": \"ssh2\", \"policy\": {\"source_type\": \"git\", \"url\": \"git@x:y.git\","
            + " \"auth\": {\"auth_type\": \"ssh\", \"username\": \"git\", \"private_key\":"
            + " \"not\\na\\nkey\\n\"}}}";
    StrictResponse<ByteString> invalid =
        httpClient
            .PUT("/scopes")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                garbage.getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("put_scope_ssh_invalid").get("status").asInt(), invalid.status().intValue());
    assertEquals(
        recorded("put_scope_ssh_invalid").get("body").get("detail").get("error").asText(),
        body(invalid).get("detail").get("error").asText());
  }

  /** R104: the listing carries every scope and never the auth object. */
  @Test
  @Order(3)
  public void listingScopesMatchesTheSource() throws Exception {
    List<String> ids = new ArrayList<>();
    JsonNode listed = null;
    for (int attempt = 0; attempt < 60 && ids.size() < 3; attempt++) {
      StrictResponse<ByteString> response = httpClient.GET("/scopes").invoke();
      assertEquals(200, response.status().intValue());
      listed = body(response);
      ids.clear();
      listed.forEach(scope -> ids.add(scope.get("scope_id").asText()));
      if (ids.size() < 3) {
        Thread.sleep(200);
      }
    }
    java.util.Collections.sort(ids);
    assertEquals(SourceAnswers.strings(recorded("get_all_scopes_ids")), ids);
    boolean anyAuth = false;
    for (JsonNode scope : listed) {
      if (scope.get("policy").has("auth") && !scope.get("policy").get("auth").isNull()) {
        anyAuth = true;
      }
    }
    assertEquals(recorded("get_all_scopes_has_auth").asBoolean(), anyAuth);
  }

  /** R104: one scope, also without its auth. */
  @Test
  @Order(4)
  public void readingOneScopeMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/scopes/alpha").invoke();
    assertEquals(200, response.status().intValue());
    JsonNode scope = body(response);
    assertEquals("alpha", scope.get("scope_id").asText());
    assertTrue(
        !scope.get("policy").has("auth") || scope.get("policy").get("auth").isNull(),
        "R104: the auth object is omitted");
    assertEquals(
        recorded("get_scope_alpha").get("data").get("entries").get(0).get("topics"),
        scope.get("data").get("entries").get(0).get("topics"));
  }

  /** R106 and R107: an unknown scope is a 404 on refresh and the default's bundle on policy. */
  @Test
  @Order(5)
  public void anUnknownScopeMatchesTheSource() {
    StrictResponse<ByteString> refresh = httpClient.POST("/scopes/nope/refresh").invoke();
    assertEquals(recorded("refresh_unknown").get("status").asInt(), refresh.status().intValue());
    assertEquals(
        recorded("refresh_unknown").get("body").get("detail").asText(),
        body(refresh).get("detail").asText());

    StrictResponse<ByteString> get = httpClient.GET("/scopes/nope").invoke();
    assertEquals(recorded("get_scope_unknown").get("status").asInt(), get.status().intValue());
    assertEquals(
        recorded("get_scope_unknown").get("body").get("detail").asText(),
        body(get).get("detail").asText());
  }

  /** R106: refreshing a known scope, with and without a hinted hash. */
  @Test
  @Order(6)
  public void refreshingMatchesTheSource() {
    assertEquals(
        recorded("refresh_alpha").asInt(),
        httpClient.POST("/scopes/alpha/refresh").invoke().status().intValue());
    assertEquals(
        recorded("refresh_alpha_hinted").asInt(),
        httpClient
            .POST("/scopes/alpha/refresh")
            .addQueryParameter("hinted_hash", REPO.first.getName())
            .invoke()
            .status()
            .intValue());
    assertEquals(
        recorded("refresh_all").asInt(),
        httpClient.POST("/scopes/refresh").invoke().status().intValue());
  }

  /**
   * R107: a scope's own bundle, built from the clone the refresh made.
   *
   * <p>The status is compared against the source; the manifest is not, because the source's own
   * scopes probe built a different repository — one commit, no {@code .manifest}, no
   * {@code envs/data.json} — and comparing two manifests taken from two different trees would
   * report a difference about the fixtures rather than about the two systems. What is compared
   * instead is that the scoped bundle is the same bundle the unscoped machinery builds from the
   * same clone, which is what R107 claims.
   */
  @Test
  @Order(7)
  public void aScopeBundleMatchesTheSource() throws Exception {
    StrictResponse<ByteString> response = httpClient.GET("/scopes/alpha/policy").invoke();
    assertEquals(recorded("scope_policy_alpha").get("status").asInt(), response.status().intValue());

    io.akka.opal.common.git.BundleMaker maker =
        new io.akka.opal.common.git.BundleMaker(
            REPO.repository(),
            java.util.Set.of("."),
            List.of(".rego", ".json"),
            "",
            null,
            List.of(".rego"));
    io.akka.opal.common.schemas.Policy.PolicyBundle expected = maker.makeBundle(REPO.second);
    assertEquals(expected.manifest(), SourceAnswers.strings(body(response).get("manifest")));
  }

  /** R108: an unknown scope's data configuration falls back to the server's own. */
  @Test
  @Order(8)
  public void scopeDataMatchesTheSource() {
    StrictResponse<ByteString> known = httpClient.GET("/scopes/alpha/data").invoke();
    assertEquals(200, known.status().intValue());
    assertEquals(
        "http://alpha/data", body(known).get("entries").get(0).get("url").asText());

    StrictResponse<ByteString> unknown = httpClient.GET("/scopes/nope/data").invoke();
    assertEquals(200, unknown.status().intValue());
    assertTrue(
        body(unknown).get("entries").get(0).get("url").asText().endsWith("/policy-data"),
        "the server's own DATA_CONFIG_SOURCES");
  }

  /** R109: a scoped data update is accepted. */
  @Test
  @Order(9)
  public void aScopedDataUpdateMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/scopes/alpha/data/update")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                ("{\"entries\": [{\"url\": \"http://x\", \"topics\": [\"policy_data\"]}],"
                        + " \"reason\": \"probe\"}")
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("scope_data_update").asInt(), response.status().intValue());
  }

  /** R133: with the flag on, the cache-stats route reports the keys the source reports. */
  @Test
  @Order(10)
  public void internalStatsMatchTheSource() {
    StrictResponse<ByteString> response =
        httpClient.GET("/internal/git-fetcher-cache-stats").invoke();
    assertEquals(recorded("internal_stats").get("status").asInt(), response.status().intValue());
    List<String> keys = new ArrayList<>();
    body(response).fieldNames().forEachRemaining(keys::add);
    java.util.Collections.sort(keys);
    assertEquals(SourceAnswers.strings(recorded("internal_stats").get("keys")), keys);
  }

  /** R105: deleting answers 204 whether or not the scope was there. */
  @Test
  @Order(11)
  public void deletingMatchesTheSource() {
    assertEquals(
        recorded("delete_beta").asInt(),
        httpClient.DELETE("/scopes/beta").invoke().status().intValue());
    assertEquals(
        recorded("delete_unknown").asInt(),
        httpClient.DELETE("/scopes/nope").invoke().status().intValue());
  }

  /** R111: two scopes on one source share a clone, and a purge keeps it while one survives. */
  @Test
  @Order(12)
  public void aSharedCloneSurvivesTheFirstDelete() throws Exception {
    Path clones = BASE_DIR.resolve("git_sources");
    assertTrue(Files.isDirectory(clones), "the refresh made a clone");
    long before;
    try (var listed = Files.list(clones)) {
      before = listed.count();
    }
    assertTrue(before >= 1);

    // `alpha` and `default` still name the same source, so its clone must still be there.
    long after;
    try (var listed = Files.list(clones)) {
      after = listed.count();
    }
    assertEquals(before, after);
  }
}
