package io.akka.opal.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.data.DataFetcher;
import io.akka.opal.client.data.DataUpdater;
import io.akka.opal.client.store.BundleUtils;
import io.akka.opal.client.store.IgnorePaths;
import io.akka.opal.client.store.OpaClient;
import io.akka.opal.client.store.PolicyStoreClient;
import io.akka.opal.client.store.TransactionLogState;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.config.Options.GitWebhookRequestParams;
import io.akka.opal.common.config.Options.SecretTypeEnum;
import io.akka.opal.common.git.BundleMaker;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.topics.Topics;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.config.ServerConfig;
import io.akka.opal.server.pubsub.Rpc;
import io.akka.opal.server.webhook.Webhooks;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;

/**
 * Runs every workload against this rebuild, and records what it answered.
 *
 * <p>The other half of {@code opal-port/bench/run_source.py}: the same workloads file, the same
 * stand-ins in the same places, and answers written in the same shape so the two files can be
 * compared row for row.
 *
 * <p>Not a test. It has no {@code @Test} method and the census does not count it; it lives under
 * {@code src/test} because it drives the rebuild the way a test does and has no business in the
 * product. Run with
 * {@code mvn exec:java -Dexec.mainClass=io.akka.opal.bench.BenchmarkRunner -Dexec.classpathScope=test}.
 *
 * <p><b>What the timings do about the JIT.</b> Every timed call reads its result into an
 * accumulator checked afterwards, so the call cannot be proven dead, and every one cycles over
 * the workload's own inputs rather than repeating one, so it cannot be proven constant and
 * hoisted out of the loop. Either alone leaves a loop the compiler is free to delete.
 */
public final class BenchmarkRunner {

  private static final Path BENCH =
      Path.of("..", "opal-port", "bench").toAbsolutePath().normalize();

  private static final ObjectNode ANSWERS = Rpc.MAPPER.createObjectNode();
  private static final ObjectNode TIMINGS = Rpc.MAPPER.createObjectNode();

  private static Map<String, JsonNode> workloads;
  private static Path repository;
  private static ObjectId first;
  private static ObjectId second;

  private static long blackhole;

  private BenchmarkRunner() {}

  public static void main(String[] args) throws Exception {
    workloads = readWorkloads();
    buildRepository();

    List<String> failures = new ArrayList<>();
    for (Map.Entry<String, ThrowingRunnable> entry : work().entrySet()) {
      try {
        entry.getValue().run();
      } catch (Exception e) {
        failures.add(entry.getKey() + ": " + e);
      }
    }

    Files.createDirectories(BENCH);
    Files.writeString(
        BENCH.resolve("port-answers.json"),
        Rpc.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(Rpc.MAPPER.createObjectNode().set("answers", sorted(ANSWERS)))
            + "\n",
        StandardCharsets.UTF_8);
    Files.writeString(
        BENCH.resolve("port-timings.json"),
        Rpc.MAPPER
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(Rpc.MAPPER.createObjectNode().set("timing", sorted(TIMINGS)))
            + "\n",
        StandardCharsets.UTF_8);

    failures.forEach(line -> System.err.println("FAILED " + line));
    System.out.println(
        ANSWERS.size() + " workload(s) answered, " + TIMINGS.size() + " timed, blackhole "
            + blackhole);
    if (!failures.isEmpty()) {
      System.exit(1);
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static Map<String, ThrowingRunnable> work() {
    Map<String, ThrowingRunnable> out = new LinkedHashMap<>();
    out.put("topic-expansion", BenchmarkRunner::topicExpansion);
    out.put("bundles", BenchmarkRunner::bundles);
    out.put("manifest-orders", BenchmarkRunner::manifestOrders);
    out.put("module-load-order", BenchmarkRunner::moduleLoadOrders);
    out.put("store-write-order", BenchmarkRunner::storeWriteOrder);
    out.put("transaction-log-health", BenchmarkRunner::transactionLogHealth);
    out.put("data-batches", BenchmarkRunner::dataBatches);
    out.put("webhook-decisions", BenchmarkRunner::webhookDecisions);
    out.put("ignore-and-glob", BenchmarkRunner::ignoreAndGlob);
    out.put("configuration-census", BenchmarkRunner::configurationCensus);
    out.put("second-batch", BenchmarkRunner::secondBatch);
    out.put("third-batch", () -> ThirdBatch.run(workloads, ANSWERS));
    out.put("fourth-batch", () -> FourthBatch.run(workloads, ANSWERS));
    return out;
  }

  private static ObjectNode sorted(ObjectNode node) {
    ObjectNode out = Rpc.MAPPER.createObjectNode();
    new TreeMap<>(Rpc.MAPPER.convertValue(node, new com.fasterxml.jackson.core.type.TypeReference<
            TreeMap<String, JsonNode>>() {}))
        .forEach(out::set);
    return out;
  }

  private static Map<String, JsonNode> readWorkloads() throws IOException {
    JsonNode list = Rpc.MAPPER.readTree(BENCH.resolve("workloads.json").toFile());
    Map<String, JsonNode> byName = new LinkedHashMap<>();
    list.forEach(workload -> byName.put(workload.get("name").asText(), workload));
    return byName;
  }

  // -- timing ---------------------------------------------------------------

  private static final long TARGET_WINDOW_NANOS = 50_000_000L;
  private static final int WINDOWS = 5;
  private static final int MAX_DOUBLINGS = 24;

  /** One reading is one reading, so five windows are taken and the median reported. */
  private static void timeIt(String name, Supplier<Long> call, String note) {
    for (int i = 0; i < 2000; i++) {
      blackhole += call.get();   // let the JIT compile the call before anything is measured
    }
    int repetitions = 1;
    for (int doubling = 0; doubling < MAX_DOUBLINGS; doubling++) {
      long started = System.nanoTime();
      for (int i = 0; i < repetitions; i++) {
        blackhole += call.get();
      }
      if (System.nanoTime() - started >= TARGET_WINDOW_NANOS) {
        break;
      }
      repetitions *= 2;
    }
    long[] readings = new long[WINDOWS];
    for (int window = 0; window < WINDOWS; window++) {
      long started = System.nanoTime();
      for (int i = 0; i < repetitions; i++) {
        blackhole += call.get();
      }
      readings[window] = (System.nanoTime() - started) / repetitions;
    }
    Arrays.sort(readings);
    ObjectNode figure = Rpc.MAPPER.createObjectNode();
    figure.put("nanosPerRun", readings[WINDOWS / 2]);
    figure.put("windowNanos", readings[WINDOWS / 2] * repetitions);
    figure.put("repetitions", repetitions);
    figure.put("windows", WINDOWS);
    figure.put("note", note);
    TIMINGS.set(name, figure);
  }

  // -- the repository -------------------------------------------------------

  private static void buildRepository() throws Exception {
    repository = Files.createTempDirectory("opal-bench-").resolve("policy-repo");
    Files.createDirectories(repository.resolve("envs"));
    try (Git git = Git.init().setDirectory(repository.toFile()).setInitialBranch("master").call()) {
      write("rbac.rego", "package rbac\n\nallow = true\n");
      write("data.json", "{\"top\": 1}\n");
      write("envs/prod.rego", "package envs.prod\n\nx = 1\n");
      write("envs/data.json", "{\"env\": \"prod\"}\n");
      write("ignored.txt", "not a policy\n");
      write(".manifest", "envs\nrbac.rego\n");
      write("envs/.manifest", "prod.rego\n");
      git.add().addFilepattern(".").call();
      first = git.commit().setMessage("first").setAuthor("bench", "b@e").setSign(false).call().getId();

      write("rbac.rego", "package rbac\n\nallow = false\n");
      write("envs/dev.rego", "package envs.dev\n\ny = 2\n");
      Files.delete(repository.resolve("envs/prod.rego"));
      git.add().addFilepattern(".").setUpdate(false).call();
      git.rm().addFilepattern("envs/prod.rego").call();
      second =
          git.commit().setMessage("second").setAuthor("bench", "b@e").setSign(false).call().getId();
    }
  }

  private static void write(String name, String content) throws IOException {
    Files.writeString(repository.resolve(name), content, StandardCharsets.UTF_8);
  }

  private static JsonNode redact(Object value) {
    String text = Rpc.MAPPER.valueToTree(value).toString();
    text = text.replace(first.getName(), "H1").replace(second.getName(), "H2");
    try {
      return Rpc.MAPPER.readTree(text);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  // -- workloads ------------------------------------------------------------

  private static void topicExpansion() {
    List<String> topics = strings(workloads.get("topic-expansion").get("topics"));
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (String topic : topics) {
      ObjectNode row = rows.addObject();
      row.put("topic", topic);
      row.set("expansion", Rpc.MAPPER.valueToTree(Topics.topicCombos(topic)));
    }
    ANSWERS.set("topic-expansion", rows);
    int[] index = {0};
    timeIt(
        "topic-expansion",
        () -> {
          long seen = 0;
          for (String topic : topics) {
            seen += Topics.topicCombos(topic).size();
          }
          index[0]++;
          return seen + index[0] % 2;
        },
        "one pass over all the topics in the workload");
  }

  private static BundleMaker maker(
      List<String> directories, List<String> extensions, List<String> ignore, String manifestPath)
      throws IOException {
    return new BundleMaker(
        io.akka.opal.common.git.RepoCloner.open(repository),
        new java.util.LinkedHashSet<>(directories),
        extensions,
        manifestPath,
        ignore,
        List.of(".rego"));
  }

  private static void bundles() throws Exception {
    BundleMaker complete = maker(List.of("."), List.of(".rego", ".json"), List.of(), "");
    BundleMaker delta = maker(List.of("."), List.of(".rego", ".json"), List.of(), "");

    ArrayNode completeRows = Rpc.MAPPER.createArrayNode();
    ObjectNode completeRow = completeRows.addObject();
    completeRow.put("step", "complete");
    completeRow.set("bundle", redact(complete.makeBundle(second)));
    ANSWERS.set("bundle-complete", completeRows);

    ArrayNode deltaRows = Rpc.MAPPER.createArrayNode();
    ObjectNode deltaRow = deltaRows.addObject();
    deltaRow.put("step", "differential");
    deltaRow.set("bundle", redact(delta.makeDiffBundle(first, second)));
    ANSWERS.set("bundle-differential", deltaRows);

    timeIt(
        "bundle-complete",
        () -> {
          try {
            return (long) complete.makeBundle(second).manifest().size();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        "one complete bundle");
    timeIt(
        "bundle-differential",
        () -> {
          try {
            return (long) delta.makeDiffBundle(first, second).manifest().size();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        },
        "one differential bundle between the same two commits");
  }

  private static void manifestOrders() throws Exception {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (String path : strings(workloads.get("manifest-orders").get("manifestPaths"))) {
      ObjectNode row = rows.addObject();
      row.put("manifestPath", path);
      row.set(
          "manifest",
          Rpc.MAPPER.valueToTree(
              maker(List.of("."), List.of(".rego", ".json"), List.of(), path)
                  .makeBundle(second)
                  .manifest()));
    }
    ANSWERS.set("manifest-orders", rows);
  }

  private static void moduleLoadOrders() {
    for (String name : List.of("module-load-order", "unnamed-modules-only")) {
      JsonNode workload = workloads.get(name);
      ArrayNode rows = Rpc.MAPPER.createArrayNode();
      for (JsonNode order : workload.get("orders")) {
        List<String> delivered = strings(order);
        List<Policy.DataModule> modules = new ArrayList<>();
        delivered.forEach(path -> modules.add(new Policy.DataModule(path, "{}")));
        Policy.PolicyBundle bundle =
            new Policy.PolicyBundle(
                strings(workload.get("manifest")), "H", null, modules, List.of(), null);
        List<String> loaded = new ArrayList<>();
        BundleUtils.sortedDataModulesToLoad(bundle).forEach(module -> loaded.add(module.path()));
        ObjectNode row = rows.addObject();
        row.set("delivered", Rpc.MAPPER.valueToTree(delivered));
        row.set("loaded", Rpc.MAPPER.valueToTree(loaded));
      }
      ANSWERS.set(name, rows);
    }
  }

  /** OPA's place, taken by something that remembers what it was asked — as the source's does. */
  private static final class RecordingEngine implements AutoCloseable {
    private final com.sun.net.httpserver.HttpServer server;
    private final List<List<String>> calls = new ArrayList<>();
    private final Map<String, String> policies = new LinkedHashMap<>();
    private final java.util.Set<String> failingOnce;

    RecordingEngine(Map<String, String> existing, java.util.Set<String> failingOnce)
        throws IOException {
      this.policies.putAll(existing);
      this.failingOnce = new java.util.HashSet<>(failingOnce);
      server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", this::handle);
      server.start();
    }

    private void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
      String method = exchange.getRequestMethod();
      String path = exchange.getRequestURI().getPath();
      exchange.getRequestBody().readAllBytes();
      int status;
      byte[] body = new byte[0];
      if (path.equals("/v1/policies") && method.equals("GET")) {
        ObjectNode out = Rpc.MAPPER.createObjectNode();
        ArrayNode array = out.putArray("result");
        policies.forEach((id, raw) -> array.addObject().put("id", id).put("raw", raw));
        body = Rpc.MAPPER.writeValueAsBytes(out);
        status = 200;
      } else if (path.startsWith("/v1/policies/")) {
        String id = path.substring("/v1/policies/".length());
        if (method.equals("PUT")) {
          calls.add(List.of("set_policy", id));
          if (failingOnce.remove(id)) {
            status = 400;
          } else {
            policies.put(id, "");
            status = 200;
          }
        } else {
          calls.add(List.of("delete_policy", id));
          policies.remove(id);
          status = 200;
        }
      } else if (path.startsWith("/v1/data")) {
        String dataPath = path.substring("/v1/data".length());
        calls.add(List.of(method.equals("DELETE") ? "delete_policy_data" : "set_policy_data",
            dataPath));
        status = 204;
      } else {
        status = 404;
      }
      exchange.getResponseHeaders().add("content-type", "application/json");
      if (status == 204) {
        exchange.sendResponseHeaders(status, -1);
      } else {
        exchange.sendResponseHeaders(status, body.length);
        if (body.length > 0) {
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        }
      }
      exchange.close();
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static OpaClient clientFor(String url) {
    return new OpaClient(
        url, null, PolicyStoreAuth.NONE, null, null, null, true, true, false, null, null, null,
        List.of(), "engine/healthcheck/opal.rego", new ConnRetryOptions(null, 0.0, 3, 0.0));
  }

  private static void storeWriteOrder() throws Exception {
    Policy.PolicyBundle complete =
        new Policy.PolicyBundle(
            List.of("b.rego", "a.rego", "d/data.json"),
            "H2",
            null,
            List.of(new Policy.DataModule("d", "{\"x\":1}")),
            List.of(
                new Policy.RegoModule("a.rego", "a", "package a\n"),
                new Policy.RegoModule("b.rego", "b", "package b\n")),
            null);
    Policy.PolicyBundle delta =
        new Policy.PolicyBundle(
            List.of("b.rego"),
            "H3",
            "H2",
            List.of(new Policy.DataModule("e", "{\"y\":2}")),
            List.of(new Policy.RegoModule("b.rego", "b", "package b2\n")),
            new Policy.DeletedFiles(List.of("gone"), List.of("a.rego")));

    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    rows.add(step("complete", Map.of("stale.rego", "package stale\n"), java.util.Set.of(), complete));
    rows.add(step("differential", Map.of("a.rego", "package a\n"), java.util.Set.of(), delta));
    rows.add(step("one-write-fails-once", Map.of(), java.util.Set.of("b.rego"), complete));
    ANSWERS.set("store-write-order", rows);

    try (RecordingEngine engine = new RecordingEngine(Map.of("stale.rego", ""), java.util.Set.of())) {
      OpaClient client = clientFor(engine.url());
      timeIt(
          "store-write-complete-bundle",
          () -> {
            client.setPolicies(complete, null);
            return (long) engine.calls.size();
          },
          "one complete bundle written into a recording engine over loopback HTTP");
    }
  }

  private static ObjectNode step(
      String name,
      Map<String, String> existing,
      java.util.Set<String> failingOnce,
      Policy.PolicyBundle bundle)
      throws Exception {
    try (RecordingEngine engine = new RecordingEngine(existing, failingOnce)) {
      clientFor(engine.url()).setPolicies(bundle, null);
      ObjectNode row = Rpc.MAPPER.createObjectNode();
      row.put("step", name);
      row.set("calls", Rpc.MAPPER.valueToTree(engine.calls));
      return row;
    }
  }

  private static void transactionLogHealth() {
    TransactionLogState state = new TransactionLogState(true, true);
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (JsonNode step : workloads.get("transaction-log-health").get("steps")) {
      if (step.get("kind").asText().equals("engine")) {
        state.setEngineReachable(step.get("reachable").asBoolean());
      } else {
        state.processTransaction(
            new Store.StoreTransaction(
                step.get("id").asText(),
                List.of(step.get("kind").asText()),
                Store.TransactionType.valueOf(step.get("type").asText()),
                step.get("success").asBoolean(),
                "",
                "t0",
                "t1",
                null));
      }
      ObjectNode row = rows.addObject();
      row.put("step", step.get("id").asText());
      row.put("ready", state.ready());
      row.put("healthy", state.healthy());
    }
    ANSWERS.set("transaction-log-health", rows);
  }

  /** A store that remembers where each write landed, in the source's own call shape. */
  private static final class TrailStore extends io.akka.opal.client.store.StubPolicyStore {
    private final List<List<Object>> calls = new ArrayList<>();

    @Override
    public void setPolicyData(JsonNode policyData, String path, String transactionId) {
      calls.add(List.of("set_policy_data", path, policyData));
    }

    @Override
    public void patchPolicyData(
        List<Store.JSONPatchAction> actions, String path, String transactionId) {
      calls.add(List.of("patch_policy_data", path, actions));
    }
  }

  private static void dataBatches() {
    for (String name : List.of("data-entries-one-batch", "data-entries-three-batches")) {
      TrailStore store = new TrailStore();
      DataFetcher fetcher =
          new DataFetcher(Http.plain(), 5) {
            @Override
            public JsonNode handleUrl(String url, Map<String, Object> config, Object inlineData) {
              return Rpc.MAPPER.createObjectNode().put("v", url);
            }
          };
      CallbacksRegister register =
          new CallbacksRegister(List.of(), Data.HttpFetcherConfig.defaultCallbackConfig());
      DataUpdater updater =
          new DataUpdater(
              store,
              fetcher,
              register,
              new CallbacksReporter(register, fetcher),
              List.of("policy_data"),
              false,
              false);

      ArrayNode rows = Rpc.MAPPER.createArrayNode();
      for (JsonNode batch : workloads.get(name).get("batches")) {
        int before = store.calls.size();
        List<Data.DataSourceEntry> entries = new ArrayList<>();
        List<String> urls = new ArrayList<>();
        for (JsonNode entry : batch) {
          urls.add(entry.get("url").asText());
          entries.add(
              new Data.DataSourceEntry(
                  entry.get("url").asText(),
                  null,
                  List.of("policy_data"),
                  entry.get("dst_path").asText(),
                  null,
                  null,
                  null));
        }
        updater.updatePolicyData(new Data.DataUpdate(null, entries, "bench", null));
        ObjectNode row = rows.addObject();
        row.set("batch", Rpc.MAPPER.valueToTree(urls));
        row.set(
            "calls",
            Rpc.MAPPER.valueToTree(store.calls.subList(before, store.calls.size())));
      }
      ANSWERS.set(name, rows);
    }
  }

  private static void webhookDecisions() {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    List<JsonNode> cases = new ArrayList<>();
    workloads.get("webhook-decisions").get("cases").forEach(cases::add);
    for (JsonNode where : cases) {
      GitWebhookRequestParams params =
          new GitWebhookRequestParams(
              "x-hub-signature-256",
              SecretTypeEnum.signature,
              "(?<= sha256=).*",
              "x-github-event",
              null,
              "push",
              where.get("matchSenderUrl").asBoolean());
      Webhooks.Decision decision =
          Webhooks.decide(
              new Webhooks.GitChanges(
                  strings(where.get("urls")),
                  where.get("branch").asText(),
                  strings(where.get("names"))),
              where.get("event").asText(),
              where.get("repoUrl").asText(),
              params,
              where.get("enforceBranch").asBoolean(),
              where.get("mainBranch").asText());
      ObjectNode row = rows.addObject();
      row.put("case", where.get("name").asText());
      if (decision.status() == null) {
        row.putNull("status");
      } else {
        row.put("status", decision.status());
      }
      row.put("trigger", decision.trigger());
      row.put("ignoredBranch", decision.ignoredBranch());
    }
    ANSWERS.set("webhook-decisions", rows);

    int[] index = {0};
    timeIt(
        "webhook-decisions",
        () -> {
          long seen = 0;
          for (JsonNode where : cases) {
            seen +=
                Webhooks.isMatchingWebhookUrl(
                        where.get("repoUrl").asText().isEmpty()
                            ? "x"
                            : where.get("repoUrl").asText(),
                        strings(where.get("urls")),
                        strings(where.get("names")))
                    ? 1
                    : 0;
          }
          index[0]++;
          return seen + index[0] % 2;
        },
        "one pass over the eight cases, matching only");
  }

  private static void ignoreAndGlob() {
    List<JsonNode> cases = new ArrayList<>();
    workloads.get("ignore-and-glob").get("cases").forEach(cases::add);
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (JsonNode where : cases) {
      ObjectNode row = rows.addObject();
      row.put("path", where.get("path").asText());
      row.set("patterns", where.get("patterns"));
      row.put(
          "ignored",
          IgnorePaths.shouldIgnorePath(
              where.get("path").asText(), strings(where.get("patterns"))));
    }
    ANSWERS.set("ignore-and-glob", rows);

    int[] index = {0};
    timeIt(
        "ignore-and-glob",
        () -> {
          long seen = 0;
          for (JsonNode where : cases) {
            seen +=
                IgnorePaths.shouldIgnorePath(
                        where.get("path").asText(), strings(where.get("patterns")))
                    ? 1
                    : 0;
          }
          index[0]++;
          return seen + index[0] % 2;
        },
        "one pass over every path in the workload");
  }

  private static void configurationCensus() {
    Map<String, String> empty = new HashMap<>();
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    ObjectNode common = rows.addObject();
    common.put("set", "common");
    common.set(
        "entries",
        Rpc.MAPPER.valueToTree(new java.util.TreeSet<>(new CommonConfig(empty).entries().keySet())));
    ObjectNode server = rows.addObject();
    server.put("set", "server");
    server.set(
        "entries",
        Rpc.MAPPER.valueToTree(new java.util.TreeSet<>(new ServerConfig(empty).entries().keySet())));
    ANSWERS.set("configuration-census", rows);
  }

  private static void secondBatch() throws Exception {
    SecondBatch.run(
        workloads,
        ANSWERS,
        io.akka.opal.common.git.RepoCloner.open(repository),
        first,
        second);
  }

  private static List<String> strings(JsonNode node) {
    List<String> out = new ArrayList<>();
    node.forEach(item -> out.add(item.asText()));
    return out;
  }
}
