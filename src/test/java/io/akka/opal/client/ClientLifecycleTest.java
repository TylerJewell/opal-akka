package io.akka.opal.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.client.store.MockPolicyStoreClient;
import io.akka.opal.client.store.PolicyStoreClientFactory;
import io.akka.opal.common.config.CommonConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SPEC-002 R20, R95, R96, R100 and R101 — what the client does before it subscribes to anything.
 *
 * <p>Start-up order is the whole of it. A client that begins fetching before the server has
 * admitted it is what {@code WAIT_ON_SERVER_LOAD} exists to prevent, and a client that starts
 * its updaters before restoring a backup answers queries from an empty engine for as long as
 * the first fetch takes.
 */
class ClientLifecycleTest {

  @AfterEach
  void clearStoreCache() {
    PolicyStoreClientFactory.clearCache();
  }

  private static ClientRuntime runtimeWith(Map<String, String> overrides) {
    Map<String, String> environment = new HashMap<>();
    environment.put("OPAL_POLICY_STORE_TYPE", "MOCK");
    environment.put("OPAL_INLINE_OPA_ENABLED", "false");
    environment.putAll(overrides);
    return new ClientRuntime(new CommonConfig(environment), new ClientConfig(environment));
  }

  /** R20: under the default scope the client subscribes by directory. */
  @Test
  void theDefaultScopeSubscribesByDirectory() {
    ClientRuntime runtime =
        runtimeWith(
            // Colon-separated, because a directory path may hold a comma — C-123.
            Map.of("OPAL_POLICY_SUBSCRIPTION_DIRS", "a:a/b:c", "OPAL_DATA_TOPICS", "t1,t2"));
    assertEquals(List.of("policy:a", "policy:c"), sorted(runtime.policyTopics()));
    assertEquals(List.of("t1", "t2"), runtime.dataTopics());
  }

  /**
   * R20: under any other scope it subscribes to one policy topic and to scope-prefixed data
   * topics, and the directory list stops being read at all.
   */
  @Test
  void anamedScopeSubscribesByScopeId() {
    ClientRuntime runtime =
        runtimeWith(
            Map.of(
                "OPAL_SCOPE_ID", "tenant1",
                "OPAL_POLICY_SUBSCRIPTION_DIRS", "a,a/b,c",
                "OPAL_DATA_TOPICS", "t1,t2"));
    assertEquals(List.of("tenant1:policy:."), runtime.policyTopics());
    assertEquals(List.of("tenant1:data:t1", "tenant1:data:t2"), runtime.dataTopics());
  }

  /** R95: nothing starts until {@code GET /loadlimit} answers 200. */
  @Test
  void theClientWaitsForTheServerToAdmitIt() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/loadlimit",
        exchange -> {
          int seen = calls.incrementAndGet();
          exchange.sendResponseHeaders(seen < 3 ? 429 : 200, -1);
          exchange.close();
        });
    server.start();
    try {
      ClientRuntime runtime =
          runtimeWith(
              Map.of(
                  "OPAL_SERVER_URL", "http://127.0.0.1:" + server.getAddress().getPort(),
                  "OPAL_POLICY_UPDATER_CONN_RETRY", "{\"wait_time\": 0.001, \"attempts\": 5}"));
      runtime.waitForServerLoadLimit();
      assertEquals(3, calls.get(), "it kept asking until the answer was 200");
    } finally {
      server.stop(0);
    }
  }

  /** R96: the engine a configuration selects — one of three answers, all three checked. */
  @Test
  void theInlineEngineIsChosenByStoreTypeAndFlag() {
    assertNull(
        runtimeWith(Map.of("OPAL_INLINE_OPA_ENABLED", "false")).buildEngineRunner(),
        "an external store runs no child process");

    assertInstanceOf(
        io.akka.opal.client.engine.OpaRunner.class,
        runtimeWith(Map.of("OPAL_POLICY_STORE_TYPE", "OPA", "OPAL_INLINE_OPA_ENABLED", "true"))
            .buildEngineRunner());

    assertInstanceOf(
        io.akka.opal.client.engine.CedarRunner.class,
        runtimeWith(
                Map.of("OPAL_POLICY_STORE_TYPE", "CEDAR", "OPAL_INLINE_CEDAR_ENABLED", "true"))
            .buildEngineRunner());

    assertNull(
        runtimeWith(Map.of("OPAL_POLICY_STORE_TYPE", "OPA", "OPAL_INLINE_CEDAR_ENABLED", "true"))
            .buildEngineRunner(),
        "the flag and the store type have to agree");
  }

  /** R100: the backup is written atomically and read back into the store. */
  @Test
  void theBackupRoundTrips(@TempDir Path work) {
    Path backup = work.resolve("nested").resolve("backup.json");
    ClientRuntime runtime =
        runtimeWith(
            Map.of(
                "OPAL_STORE_BACKUP_PATH", backup.toString(),
                "OPAL_POLICY_STORE_TYPE", "MOCK"));

    MockPolicyStoreClient store = (MockPolicyStoreClient) runtime.store();
    store.setPolicyData(io.akka.opal.server.pubsub.Rpc.MAPPER.createObjectNode().put("v", 1),
        "/users", null);

    runtime.backupStore();
    assertTrue(Files.isRegularFile(backup), "written to the path it was given");

    ClientRuntime second =
        runtimeWith(
            Map.of(
                "OPAL_STORE_BACKUP_PATH", backup.toString(),
                "OPAL_POLICY_STORE_TYPE", "MOCK"));
    second.loadStoreFromBackup();
    assertTrue(second.backupLoaded());
    assertEquals(1, second.store().getData("/users").get("v").asInt());
  }

  /** R100: a missing backup file is a warning and an empty store, not a failure to start. */
  @Test
  void aMissingBackupIsNotAFailure(@TempDir Path work) {
    ClientRuntime runtime =
        runtimeWith(Map.of("OPAL_STORE_BACKUP_PATH", work.resolve("absent.json").toString()));
    runtime.loadStoreFromBackup();
    assertFalse(runtime.backupLoaded());
  }

  /** R100: nothing half-written is ever visible under the backup's own name. */
  @Test
  void theBackupIsWrittenThroughATemporaryFile(@TempDir Path work) throws IOException {
    Path backup = work.resolve("backup.json");
    Files.writeString(backup, "{\"old\": true}", StandardCharsets.UTF_8);

    ClientRuntime runtime = runtimeWith(Map.of("OPAL_STORE_BACKUP_PATH", backup.toString()));
    runtime.backupStore();

    assertFalse(Files.readString(backup).contains("\"old\""), "replaced, not appended to");
    try (var listing = Files.list(work)) {
      assertEquals(
          List.of("backup.json"),
          listing.map(path -> path.getFileName().toString()).sorted().toList(),
          "no temporary file left behind");
    }
  }

  /** R101: offline mode is refused against an engine this process does not run. */
  @Test
  void offlineModeIsRefusedWithoutAnInlineEngine(@TempDir Path work) {
    ClientRuntime runtime =
        runtimeWith(
            Map.of(
                "OPAL_OFFLINE_MODE_ENABLED", "true",
                "OPAL_INLINE_OPA_ENABLED", "false",
                "OPAL_STORE_BACKUP_PATH", work.resolve("b.json").toString()));
    assertFalse(runtime.offlineModeEnabled(), "refused, with the warning the source logs");
  }

  /** R101's other side: with an inline engine, offline mode stays on. */
  @Test
  void offlineModeStandsWithAnInlineEngine(@TempDir Path work) {
    ClientRuntime runtime =
        runtimeWith(
            Map.of(
                "OPAL_OFFLINE_MODE_ENABLED", "true",
                "OPAL_INLINE_OPA_ENABLED", "true",
                "OPAL_POLICY_STORE_TYPE", "MOCK",
                "OPAL_STORE_BACKUP_PATH", work.resolve("b.json").toString()));
    assertTrue(runtime.offlineModeEnabled());
  }

  private static List<String> sorted(List<String> values) {
    return values.stream().sorted().toList();
  }
}
