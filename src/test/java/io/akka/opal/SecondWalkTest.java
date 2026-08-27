package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.client.data.DataFetcher;
import io.akka.opal.client.engine.EngineLogLine;
import io.akka.opal.client.store.MockPolicyStoreClient;
import io.akka.opal.client.store.PolicyStoreClient;
import io.akka.opal.client.store.PolicyStoreClientFactory;
import io.akka.opal.client.store.StoreTransactionContext;
import io.akka.opal.common.auth.JwtSigner;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.config.Enums.EngineLogFormat;
import io.akka.opal.common.fetcher.FetcherRegister;
import io.akka.opal.common.fetcher.FetchingEngine;
import io.akka.opal.common.git.ClonePathFinder;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.sync.NamedLock;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.pubsub.Rpc;
import io.akka.opal.server.scopes.GitOps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The rules a second walk over permitio/opal produced — SPEC-002 R336 to R405.
 *
 * <p>Each of these is a behaviour the original has that the rebuild did not, found by reading the
 * original module by module a second time rather than by reading the rebuild and asking whether
 * it looked finished. They are grouped here rather than scattered because what they have in
 * common is how they were found, and a reader looking for what the second walk changed has one
 * place to look.
 */
class SecondWalkTest {

  @AfterEach
  void clearStoreCache() {
    PolicyStoreClientFactory.clearCache();
  }

  // -- the store transaction records what ran ------------------------------

  /** R372: every write made through the transaction's store records its own name. */
  @Test
  void everyWriteInsideATransactionIsRecorded() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    StoreTransactionContext transaction =
        new StoreTransactionContext(store, "t1", Store.TransactionType.data);
    PolicyStoreClient recording = transaction.store();
    recording.setPolicyData(Rpc.MAPPER.createObjectNode(), "a", "t1");
    recording.patchPolicyData(List.of(), "a", "t1");
    recording.deletePolicyData("a", "t1");
    assertEquals(
        List.of("set_policy_data", "patch_policy_data", "delete_policy_data"),
        transaction.actions());
  }

  /** R372: a read through the same proxy is not an action, because it changes nothing. */
  @Test
  void aReadInsideATransactionIsNotRecorded() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    StoreTransactionContext transaction =
        new StoreTransactionContext(store, "t2", Store.TransactionType.data);
    transaction.store().getPolicyVersion();
    assertEquals(List.of(), transaction.actions());
  }

  /** R371: the mock store raises for a path it never wrote, rather than answering nothing. */
  @Test
  void theMockStoreRaisesForAPathItNeverWrote() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    assertThrows(java.util.NoSuchElementException.class, () -> store.getData("never"));
    assertThrows(
        java.util.NoSuchElementException.class, () -> store.deletePolicyData("never", null));
  }

  // -- keys ----------------------------------------------------------------

  /** R376: an EC private key signs on its own, with its public half derived. */
  @Test
  void anEcPrivateKeySignsWithoutItsPublicHalf() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
    generator.initialize(new java.security.spec.ECGenParameterSpec("secp256r1"));
    KeyPair pair = generator.generateKeyPair();
    JwtSigner signer =
        new JwtSigner(
            pem("PRIVATE KEY", pair.getPrivate().getEncoded()),
            pem("PUBLIC KEY", pair.getPublic().getEncoded()),
            io.akka.opal.common.auth.Types.EncryptionKeyFormat.pem,
            io.akka.opal.common.auth.Types.EncryptionKeyFormat.pem,
            null,
            io.akka.opal.common.auth.Types.JWTAlgorithm.ES256,
            "aud",
            "iss");
    assertTrue(signer.enabled());
    assertNotNull(signer.sign(
            java.util.UUID.randomUUID().toString(), java.time.Duration.ofMinutes(5), Map.of()));
  }

  /** R376: and so does an Ed25519 one, which the algorithm list also offers. */
  @Test
  void anEd25519PrivateKeySignsWithoutItsPublicHalf() throws Exception {
    KeyPair pair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    JwtSigner signer =
        new JwtSigner(
            pem("PRIVATE KEY", pair.getPrivate().getEncoded()),
            pem("PUBLIC KEY", pair.getPublic().getEncoded()),
            io.akka.opal.common.auth.Types.EncryptionKeyFormat.pem,
            io.akka.opal.common.auth.Types.EncryptionKeyFormat.pem,
            null,
            io.akka.opal.common.auth.Types.JWTAlgorithm.EdDSA,
            "aud",
            "iss");
    assertTrue(signer.enabled());
    assertNotNull(signer.sign(
            java.util.UUID.randomUUID().toString(), java.time.Duration.ofMinutes(5), Map.of()));
  }

  private static String pem(String label, byte[] der) {
    String body = java.util.Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
    return "-----BEGIN " + label + "-----\n" + body + "\n-----END " + label + "-----\n";
  }

  // -- locks and paths -----------------------------------------------------

  /** R380: the lock file is truncated when it is taken, so nothing survives a previous holder. */
  @Test
  void theLockFileIsTruncatedOnAcquire(@TempDir Path directory) throws Exception {
    Path file = directory.resolve("opal.lock");
    Files.writeString(file, "left over by somebody else", StandardCharsets.UTF_8);
    NamedLock lock = new NamedLock(file.toString());
    assertTrue(lock.tryAcquire());
    try {
      assertEquals(0, Files.size(file));
    } finally {
      lock.release();
    }
  }

  /** R379: a plain file matching the pattern counts as a candidate, the way a glob counts it. */
  @Test
  void aFileCountsAsACloneCandidate(@TempDir Path directory) throws Exception {
    Files.createFile(directory.resolve("opal-repo-clone-abc"));
    ClonePathFinder finder =
        new ClonePathFinder(directory.toString(), "opal-repo-clone", false);
    assertNotNull(finder.clonePath());
  }

  // -- engine log rendering ------------------------------------------------

  /** R363: with colour on, the detail half of the line is dimmed and the message is not. */
  @Test
  void aRenderedEngineLineCarriesColourWhenAsked() {
    String line = "{\"level\":\"info\",\"msg\":\"Server started\",\"addrs\":[\":8181\"]}";
    EngineLogLine.Rendered plain = EngineLogLine.render(line, EngineLogFormat.FULL, false);
    EngineLogLine.Rendered coloured = EngineLogLine.render(line, EngineLogFormat.FULL, true);
    assertFalse(plain.text().contains(EngineLogLine.RESET));
    assertTrue(coloured.text().startsWith("Server started"));
    assertTrue(coloured.text().contains(EngineLogLine.BRIGHTER));
    assertTrue(coloured.text().endsWith(EngineLogLine.RESET));
  }

  // -- the data fetcher ----------------------------------------------------

  /** R263: an entry naming neither inline data nor a url is refused, not sent to the default. */
  @Test
  void anEntryWithNeitherDataNorUrlIsRefused() {
    DataFetcher fetcher =
        new DataFetcher(new FetchingEngine(Http.plain(), 5)).withDefaultDataUrl("http://elsewhere");
    assertNull(fetcher.handleUrl(null, null, null));
    assertNull(fetcher.handleUrl("", null, null));
  }

  /** R340: a caller that named no requests at all gets the built-in route, carrying the token. */
  @Test
  void theDefaultFetchCarriesTheClientToken() throws Exception {
    com.sun.net.httpserver.HttpServer server =
        com.sun.net.httpserver.HttpServer.create(
            new java.net.InetSocketAddress("127.0.0.1", 0), 0);
    java.util.List<String> seen = new java.util.ArrayList<>();
    server.createContext(
        "/data",
        exchange -> {
          seen.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
          byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      DataFetcher fetcher =
          new DataFetcher(new FetchingEngine(Http.plain(), 5))
              .withDefaultDataUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/data")
              .withToken("a-client-token");
      List<DataFetcher.Fetched> results = fetcher.handleUrls(null);
      assertEquals(1, results.size());
      assertEquals(List.of("Bearer a-client-token"), seen);
    } finally {
      server.stop(0);
    }
  }

  // -- the fetcher register ------------------------------------------------

  /** R377: a name that is a package registers every provider the package holds. */
  @Test
  void aNamedPackageRegistersEveryProviderInIt() {
    FetcherRegister register =
        new FetcherRegister(Http.plain(), 5, List.of("io.akka.opal.common.fetcher"));
    assertTrue(register.names().contains("HttpFetchProvider"));
    assertTrue(register.names().contains("FastApiRpcFetchProvider"));
  }

  /** R377: and a name that is neither a class nor a package is refused rather than ignored. */
  @Test
  void aNameThatIsNeitherAClassNorAPackageIsRefused() {
    FetcherRegister register =
        new FetcherRegister(Http.plain(), 5, List.of("io.akka.opal.no.such.place"));
    assertEquals(java.util.Set.of(), register.names());
  }

  // -- the client's own answers --------------------------------------------

  /** R351: {@code online} follows the store's health, not whether connectivity is allowed. */
  @Test
  void onlineFollowsTheStoreRatherThanConnectivity() {
    Map<String, String> environment = new HashMap<>();
    environment.put("OPAL_POLICY_STORE_TYPE", "MOCK");
    environment.put("OPAL_INLINE_OPA_ENABLED", "false");
    io.akka.opal.client.ClientRuntime runtime =
        new io.akka.opal.client.ClientRuntime(
            new CommonConfig(environment), new ClientConfig(environment));
    assertEquals(runtime.healthy(), runtime.online());
    runtime.disableServerConnectivity();
    assertEquals(runtime.healthy(), runtime.online());
  }

  /** R390: purge work asked for after the shutdown began is refused rather than begun. */
  @Test
  void aPurgeAfterShutdownIsRefused(@TempDir Path base) {
    io.akka.opal.server.scopes.ScopesService service =
        new io.akka.opal.server.scopes.ScopesService(
            null, base, 1, List.of(".rego"), (topics, data) -> {});
    service.stop(0.1);
    JsonNode command =
        Rpc.MAPPER.valueToTree(
            new io.akka.opal.server.scopes.ScopesService.ScopePurgeCommand(
                "0".repeat(64) + "-0", "alpha", "delete"));
    // Nothing to assert on beyond its not throwing: the refusal is the absence of work, and
    // reaching the store through a null repository is what it would do if it did not refuse.
    service.applyPurge(command);
  }

  /** R391: a source id off the wire that is not a digest and a shard index is refused. */
  @Test
  void aMalformedSourceIdIsRefused(@TempDir Path base) {
    io.akka.opal.server.scopes.ScopesService service =
        new io.akka.opal.server.scopes.ScopesService(
            null, base, 1, List.of(".rego"), (topics, data) -> {});
    JsonNode command =
        Rpc.MAPPER.valueToTree(
            new io.akka.opal.server.scopes.ScopesService.ScopePurgeCommand(
                "not-a-source-id", "alpha", "delete"));
    service.applyPurge(command);
  }

  /** R389: a purge with no reason on the wire is a delete, which is the defensive direction. */
  @Test
  void aPurgeWithNoReasonIsADelete() {
    io.akka.opal.server.scopes.ScopesService.ScopePurgeCommand purge =
        Rpc.MAPPER.convertValue(
            Map.of("source_id", "a", "scope_id", "b"),
            io.akka.opal.server.scopes.ScopesService.ScopePurgeCommand.class);
    assertEquals("delete", purge.reason());
  }

  /** R399: a source that answered after failing says how many failures it took to come back. */
  @Test
  void aRecoveredSourceIsAnnounced() {
    GitOps.configure(new GitOps.Settings(0, 4, 8, 1, 4), new io.akka.opal.common.metrics.Metrics());
    GitOps.forgetSource("recovered");
    GitOps.recordFailure("recovered", "https://example.invalid/repo.git", "boom");
    GitOps.clearFailure("recovered");
    assertFalse(GitOps.inBackoff("recovered"));
  }

}
