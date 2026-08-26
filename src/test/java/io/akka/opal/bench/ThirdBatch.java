package io.akka.opal.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.client.engine.EngineLogLine;
import io.akka.opal.client.engine.EngineRunner;
import io.akka.opal.common.config.Enums.EngineLogFormat;
import io.akka.opal.common.git.ClonePathFinder;
import io.akka.opal.common.git.Tar;
import io.akka.opal.common.logging.Loguru;
import io.akka.opal.common.metrics.StatsdClient;
import io.akka.opal.common.schemas.PolicySource;
import io.akka.opal.server.pubsub.Rpc;
import io.akka.opal.server.scopes.GitOps;
import io.akka.opal.server.scopes.GitPolicyFetcher;
import java.io.ByteArrayInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The workloads the completeness pass added, on the rebuild's side.
 *
 * <p>Each drives the rebuild's own code over the same inputs {@code run_source.py} drives the
 * original's, so what the benchmark compares is two systems answering rather than one system
 * against a recording of the other.
 */
final class ThirdBatch {

  private ThirdBatch() {}

  static void run(Map<String, JsonNode> workloads, ObjectNode answers) throws Exception {
    statsdWire(answers);
    engineLogFormats(workloads, answers);
    engineRelaunchBackoff(answers);
    logRendering(workloads, answers);
    scopeShardIndex(workloads, answers);
    gitBackoffSchedule(answers);
    tarSafety(answers);
    clonePathDiscovery(answers);
  }

  /** R154–R158: the datagrams a fixed set of measurements becomes. */
  private static void statsdWire(ObjectNode answers) throws Exception {
    List<String> received = new ArrayList<>();
    try (DatagramSocket socket = new DatagramSocket(0)) {
      socket.setSoTimeout(400);
      int port = socket.getLocalPort();
      try (StatsdClient client = new StatsdClient("127.0.0.1", port, "permit.opal")) {
        client.increment("startup", null);
        client.increment("scope.clone", tags("outcome", "ok", "scope_id", "s1"));
        client.decrement("in_flight", null);
        client.gauge("opal_server.scopes.count", 3L, null);
        client.gauge("with.tags", 1.5, tags("a", "b"));
        client.event("title here", "message body", tags("k", "v"));
        client.event("no tags", "body", null);
        client.event("multi", "first\nsecond", null);
        client.increment("empty_value_tag", tags("k", ""));
        client.gauge("neg", -1.25, null);
      }
      byte[] buffer = new byte[65535];
      for (int i = 0; i < 10; i++) {
        try {
          DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
          socket.receive(packet);
          received.add(
              new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8));
        } catch (java.net.SocketTimeoutException e) {
          break;
        }
      }
    }
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    received.forEach(line -> rows.addObject().put("datagram", line));
    answers.set("statsd-wire", rows);
  }

  private static Map<String, String> tags(String... pairs) {
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      out.put(pairs[i], pairs[i + 1]);
    }
    return out;
  }

  /** R163–R166: what each engine log format renders, line by line. */
  private static void engineLogFormats(Map<String, JsonNode> workloads, ObjectNode answers) {
    List<String> lines = strings(workloads.get("engine-log-formats").get("lines"));
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (String format : List.of("minimal", "http", "full", "none")) {
      EngineLogFormat chosen = EngineLogFormat.valueOf(format.toUpperCase());
      for (String line : lines) {
        EngineLogLine.Rendered rendered = EngineLogLine.render(line, chosen);
        ObjectNode row = rows.addObject();
        row.put("format", format);
        row.put("line", line);
        row.put("level", rendered == null ? null : rendered.level());
        row.put("text", rendered == null ? null : rendered.text());
      }
    }
    answers.set("engine-log-formats", rows);
  }

  /** R168: how long the client waits before launching the engine again. */
  private static void engineRelaunchBackoff(ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (int attempt = 1; attempt <= 9; attempt++) {
      rows.addObject()
          .put("attempt", attempt)
          .put("seconds", EngineRunner.relaunchDelay(attempt) / 1000.0);
    }
    answers.set("engine-relaunch-backoff", rows);
  }

  /** R170–R177: a record rendered through the format, and a name shortened to fit. */
  private static void logRendering(Map<String, JsonNode> workloads, ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (JsonNode entry : workloads.get("log-rendering").get("records")) {
      String name = entry.get("name").asText();
      String level = entry.get("level").asText();
      String message = entry.get("message").asText();
      Loguru.Record record =
          new Loguru.Record(
              ZonedDateTime.of(2026, 1, 2, 3, 4, 5, 678901000, ZoneOffset.UTC),
              4242,
              "MainThread",
              7,
              name,
              level,
              message,
              null,
              "render",
              "bench.py",
              1,
              "bench",
              1.5);
      ObjectNode row = rows.addObject();
      row.put("name", name);
      row.put("shortened", Loguru.shortenName(name));
      row.put("without_pid", Loguru.render(Loguru.FORMAT_WITHOUT_PID, record, false));
      row.put("with_pid", Loguru.render(Loguru.FORMAT_WITH_PID, record, false));
      row.put(
          "allowed",
          Loguru.allowed(name, List.of("uvicorn.protocols.http"), List.of("uvicorn")));
    }
    answers.set("log-rendering", rows);
  }

  /** R227: which clone directory a scope's branch lands in. */
  private static void scopeShardIndex(Map<String, JsonNode> workloads, ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (String branch : strings(workloads.get("scope-shard-index").get("branches"))) {
      for (int shards : List.of(1, 3, 8)) {
        PolicySource.GitPolicyScopeSource source =
            new PolicySource.GitPolicyScopeSource(
                "git",
                "https://example.com/policy.git",
                PolicySource.NoAuthData.get(),
                List.of("."),
                List.of(".rego"),
                null,
                ".manifest",
                true,
                branch);
        rows.addObject()
            .put("branch", branch)
            .put("shards", shards)
            .put("source_id", GitPolicyFetcher.sourceId(source, shards));
      }
    }
    answers.set("scope-shard-index", rows);
  }

  /** R222: the delay armed after each consecutive failure of a source. */
  private static void gitBackoffSchedule(ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    double[][] settings = {{10, 0}, {10, 25}, {10, 3}, {0, 0}, {2.5, 0}};
    for (double[] pair : settings) {
      GitOps.configure(new GitOps.Settings(0, 4, 0, pair[0], pair[1]), null);
      for (int failures = 1; failures <= 6; failures++) {
        rows.addObject()
            .put("base", pair[0])
            .put("cap", pair[1])
            .put("failures", failures)
            .put("seconds", GitOps.backoffDelay(failures));
      }
    }
    GitOps.reset();
    answers.set("git-backoff-schedule", rows);
  }

  /** R191: which archive members a bundle reader accepts. */
  private static void tarSafety(ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    Object[][] members = {
      {"rbac.rego", '0', ""},
      {"envs/", '5', ""},
      {"link", '1', "../../etc/passwd"},
      {"symlink", '2', "../../etc/passwd"},
      {"chardev", '3', ""},
      {"blockdev", '4', ""},
    };
    for (Object[] member : members) {
      String outcome;
      try {
        List<String> seen = new ArrayList<>();
        Tar.forEach(
            new ByteArrayInputStream(
                header((String) member[0], (Character) member[1], (String) member[2])),
            (name, directory, size, content) -> seen.add(name));
        outcome = "accepted:" + seen;
      } catch (Exception e) {
        outcome = "refused:" + e.getMessage();
      }
      rows.addObject()
          .put("member", (String) member[0])
          .put("typeflag", String.valueOf(member[1]))
          .put("outcome", outcome);
    }
    answers.set("tar-safety", rows);
  }

  private static byte[] header(String name, char typeFlag, String linkName) {
    byte[] block = new byte[1024];
    put(block, 0, name);
    put(block, 100, "0000644");
    put(block, 124, "00000000000");
    put(block, 136, "00000000000");
    block[156] = (byte) typeFlag;
    put(block, 157, linkName);
    put(block, 257, "ustar");
    for (int i = 148; i < 156; i++) {
      block[i] = ' ';
    }
    int checksum = 0;
    for (byte b : block) {
      checksum += b & 0xff;
    }
    put(block, 148, String.format("%06o", checksum) + "\0 ");
    return block;
  }

  private static void put(byte[] block, int offset, String value) {
    byte[] raw = value.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(raw, 0, block, offset, raw.length);
  }

  /** R188–R190: where a clone goes, and what is found afterwards. */
  private static void clonePathDiscovery(ObjectNode answers) throws Exception {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (boolean fixed : List.of(false, true)) {
      Path base = Files.createTempDirectory("opal-bench-clone");
      try {
        ClonePathFinder finder = new ClonePathFinder(base.toString(), "opal_repo_clone", fixed);
        ObjectNode row = rows.addObject();
        row.put("fixed", fixed);
        row.put("before_any_clone", finder.clonePath() == null ? "none" : "found");
        Path first = finder.createNewClonePath();
        Files.writeString(first.resolve("marker"), "one");
        row.put("first_name_shape", shape(first.getFileName().toString()));
        row.put("found_again", first.equals(finder.clonePath()));
        Path second = finder.createNewClonePath();
        row.put("second_is_the_same", first.equals(second));
        row.put("first_survived", Files.exists(first.resolve("marker")));
        row.put("directories_left", (int) Files.list(base).count());
      } finally {
        io.akka.opal.common.git.RepoCloner.deleteRecursively(base);
      }
    }
    answers.set("clone-path-discovery", rows);
  }

  /** A directory name with its random half replaced, so two runs can be compared. */
  private static String shape(String name) {
    return name.replaceAll("-[0-9a-f]{32}$", "-<random>");
  }

  private static List<String> strings(JsonNode node) {
    List<String> out = new ArrayList<>();
    node.forEach(item -> out.add(item.asText()));
    return out;
  }
}
