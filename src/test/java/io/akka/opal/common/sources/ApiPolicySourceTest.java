package io.akka.opal.common.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.config.Enums.PolicyBundleServerType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SPEC-002 R40 to R42 — a bundle server watched for new bundles, against a real one.
 *
 * <p>The two ways of noticing a change are not interchangeable and the source runs both: an ETag
 * lets the server answer 304 without sending anything, and where it offers none the client
 * downloads the whole bundle and hashes it. The sequence matters as much as either answer — the
 * same bytes twice must be "no change", and so must the first fetch of all, when there is nothing
 * to compare against.
 */
class ApiPolicySourceTest {

  /** A bundle server that can be given new bytes and an ETag mid-run, as the source's probe did. */
  private static final class BundleServer implements AutoCloseable {
    private final HttpServer server;
    private volatile byte[] body;
    private volatile String etag;
    private volatile int hits;

    BundleServer(byte[] initial) throws IOException {
      this.body = initial;
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            hits++;
            String ifNoneMatch = exchange.getRequestHeaders().getFirst("If-None-Match");
            if (etag != null && etag.equals(ifNoneMatch)) {
              exchange.sendResponseHeaders(304, -1);
              exchange.close();
              return;
            }
            exchange.getResponseHeaders().add("content-type", "application/gzip");
            if (etag != null) {
              exchange.getResponseHeaders().add("ETag", etag);
            }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
          });
      server.start();
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  /**
   * One file in a gzipped USTAR archive, written here rather than taken from a library so the
   * bytes under test are the bytes this test wrote.
   */
  private static byte[] tarGz(String rego) throws IOException {
    byte[] content = rego.getBytes(StandardCharsets.UTF_8);
    byte[] header = new byte[512];
    put(header, 0, "rbac.rego");
    put(header, 100, "000644 ");
    put(header, 108, "000000 ");
    put(header, 116, "000000 ");
    put(header, 124, String.format("%011o ", content.length));
    put(header, 136, "00000000000 ");
    java.util.Arrays.fill(header, 148, 156, (byte) ' ');
    header[156] = '0';
    put(header, 257, "ustar");
    put(header, 263, "00");
    int checksum = 0;
    for (byte b : header) {
      checksum += b & 0xff;
    }
    put(header, 148, String.format("%06o", checksum));
    header[154] = 0;
    header[155] = ' ';

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
      gzip.write(header);
      gzip.write(content);
      gzip.write(new byte[(512 - content.length % 512) % 512]);
      gzip.write(new byte[1024]);
    }
    return bytes.toByteArray();
  }

  private static void put(byte[] block, int offset, String value) {
    byte[] raw = value.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(raw, 0, block, offset, raw.length);
  }

  private static ApiPolicySource source(BundleServer server, Path work) {
    return new ApiPolicySource(
        server.url(),
        work.resolve("clone").toString(),
        0,
        null,
        null,
        "us-east-1",
        PolicyBundleServerType.HTTP,
        work.resolve("bundle.tar.gz").toString(),
        "*");
  }

  /** R40, R41: the whole sequence the source's probe ran, in order, against a real server. */
  @Test
  void changeDetectionFollowsTheSourcesSequence(@TempDir Path work) throws Exception {
    JsonNode recorded = SourceAnswers.get("api_bundle_source");

    try (BundleServer server = new BundleServer(tarGz("package rbac\nallow = true\n"))) {
      ApiPolicySource source = source(server, work);

      source.getInitialPolicyStateFromRemote();
      assertEquals(recorded.get("after_initial_hits").asInt(), server.hits);
      assertTrue(recorded.get("initial_hash").asBoolean(), "the source had a hash after one fetch");
      assertNotNull(source.repository(), "the bundle became a local repository");

      ApiPolicySource.Updated same = source.apiUpdatePolicy();
      assertEquals(recorded.get("same_bundle_has_changes").asBoolean(), same.changed());

      server.body = tarGz("package rbac\nallow = false\n");
      ApiPolicySource.Updated moved = source.apiUpdatePolicy();
      assertEquals(recorded.get("new_bundle_has_changes").asBoolean(), moved.changed());
      assertTrue(recorded.get("hash_changed").asBoolean(), "the source's version moved too");

      // R41: the version the commit is named after. The digest is of the archive each side's own
      // tar writer produced, so the two cannot share one; what is compared is the message the
      // source built around it and the shape of what it put there.
      String recordedMessage = recorded.get("commit_message").asText();
      assertTrue(recordedMessage.startsWith("new version "), recordedMessage);
      assertTrue(
          recordedMessage.substring("new version ".length()).matches("[0-9a-f]{64}"),
          "the source named the version by the archive's digest");
      assertTrue(moved.version().matches("[0-9a-f]{64}"), moved.version());

      server.etag = "W/\"v1\"";
      ApiPolicySource.Updated firstWithEtag = source.apiUpdatePolicy();
      assertEquals(recorded.get("etag_first_has_changes").asBoolean(), firstWithEtag.changed());

      ApiPolicySource.Updated secondWithEtag = source.apiUpdatePolicy();
      assertEquals(recorded.get("etag_second_has_changes").asBoolean(), secondWithEtag.changed());
      assertEquals(recorded.get("etag_value").asText(), secondWithEtag.version());
    }
  }

  /**
   * R40 has two levels and they answer differently on the first fetch: the download found bytes it
   * did not have, and the update is still no change, because there is no version to move on from.
   * Reading the first as the second puts a redundant commit into the repository the bundle route
   * serves, and every connected client is then told to fetch it.
   */
  @Test
  void theFirstFetchFindsBytesAndIsStillNotAnUpdate(@TempDir Path work) throws Exception {
    try (BundleServer server = new BundleServer(tarGz("package a\n"))) {
      ApiPolicySource source = source(server, work);
      assertTrue(source.fetchPolicyBundle().changed(), "the download had nothing to compare to");
      assertFalse(source.fetchPolicyBundle().changed(), "the same bytes twice");
    }
  }

  /** R40: the first fetch from a server that has just turned ETags on is not an update either. */
  @Test
  void aServerTurningEtagsOnMidLifeIsNotAnUpdate(@TempDir Path work) throws Exception {
    try (BundleServer server = new BundleServer(tarGz("package a\n"))) {
      ApiPolicySource source = source(server, work);
      source.getInitialPolicyStateFromRemote();
      server.etag = "W/\"v9\"";
      assertFalse(source.apiUpdatePolicy().changed());
    }
  }

  /** R42: the header set each server type authenticates with. */
  @Test
  void theAuthenticationHeaderSetsAreTheSources(@TempDir Path work) {
    JsonNode recorded = SourceAnswers.get("api_bundle_source").get("auth_headers");

    ApiPolicySource s3 =
        new ApiPolicySource(
            "https://bucket.s3.amazonaws.com/prefix", work.resolve("c").toString(), 0, "secret",
            "AKIA", "us-east-1", PolicyBundleServerType.AWS_S3,
            work.resolve("b.tar.gz").toString(), "*");
    List<String> s3Headers = new ArrayList<>(s3.buildAuthHeaders("bundle.tar.gz").keySet());
    java.util.Collections.sort(s3Headers);
    assertEquals(
        SourceAnswers.strings(recorded.get("PolicyBundleServerType.AWS_S3/AKIA")), s3Headers);

    ApiPolicySource anonymous =
        new ApiPolicySource(
            "http://x", work.resolve("c2").toString(), 0, null, null, "us-east-1",
            PolicyBundleServerType.HTTP, work.resolve("b2.tar.gz").toString(), "*");
    assertEquals(
        SourceAnswers.strings(recorded.get("PolicyBundleServerType.HTTP/None")),
        new ArrayList<>(anonymous.buildAuthHeaders("bundle.tar.gz").keySet()));

    ApiPolicySource bearer =
        new ApiPolicySource(
            "http://x", work.resolve("c3").toString(), 0, "tok", null, "us-east-1",
            PolicyBundleServerType.HTTP, work.resolve("b3.tar.gz").toString(), "*");
    assertEquals(
        List.of("Authorization"),
        new ArrayList<>(bearer.buildAuthHeaders("bundle.tar.gz").keySet()));
  }

  /** R41: each new bundle becomes one commit, named after the version the server gave it. */
  @Test
  void eachNewBundleBecomesACommitNamedAfterItsVersion(@TempDir Path work) throws Exception {
    try (BundleServer server = new BundleServer(tarGz("package rbac\nallow = true\n"))) {
      ApiPolicySource source = source(server, work);
      source.getInitialPolicyStateFromRemote();
      assertTrue(source.ready());
      assertTrue(
          Files.exists(work.resolve("clone").resolve("rbac.rego")),
          "the bundle's own file is in the clone");

      List<String> announced = new ArrayList<>();
      source.addOnNewPolicyCallback((from, to) -> announced.add(to.name()));

      server.body = tarGz("package rbac\nallow = false\n");
      source.checkForChanges();

      assertEquals(1, announced.size(), "one new bundle, one announcement");
      try (org.eclipse.jgit.api.Git git = org.eclipse.jgit.api.Git.wrap(source.repository())) {
        String message = git.log().setMaxCount(1).call().iterator().next().getFullMessage().trim();
        assertTrue(message.startsWith("new version "), message);
        assertTrue(
            message.substring("new version ".length()).matches("[0-9a-f]{64}"),
            "the version is the archive's digest, which is the shape the source records");
      }
      assertEquals(
          "package rbac\nallow = false\n",
          Files.readString(work.resolve("clone").resolve("rbac.rego")));
    }
  }
}
