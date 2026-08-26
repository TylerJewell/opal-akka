package io.akka.opal.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.cli.OpalCli;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.client.store.OpaClient;
import io.akka.opal.client.store.TransactionLogPolicyWriter;
import io.akka.opal.client.store.TransactionLogState;
import io.akka.opal.common.auth.JwtSigner;
import io.akka.opal.common.auth.Types.EncryptionKeyFormat;
import io.akka.opal.common.auth.Types.JWTAlgorithm;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.git.PolicyUpdates;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.topics.Topics;
import io.akka.opal.common.util.Hashing;
import io.akka.opal.common.util.Paths2;
import io.akka.opal.server.config.ServerConfig;
import io.akka.opal.server.pubsub.Rpc;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;

/**
 * The second batch of benchmark workloads, kept apart so the first stays readable.
 *
 * <p>Nine workloads over the parts of the product the first batch does not reach: every
 * configuration entry's printed value, the directory half of addressing, what a policy change
 * announces, the report hash, a token round trip and its refusals, the six ways a policy store
 * can be told to authenticate, the health policy as text, the callbacks register, and the
 * command line.
 */
final class SecondBatch {

  private SecondBatch() {}

  static void run(
      Map<String, JsonNode> workloads,
      ObjectNode answers,
      Repository repository,
      ObjectId first,
      ObjectId second)
      throws Exception {
    configurationValues(answers);
    topicDirectories(workloads, answers);
    policyUpdateNotifications(answers, repository, first, second);
    reportHashes(workloads, answers);
    jwtRoundTrip(answers);
    storeAuthModes(answers);
    healthPolicy(answers);
    callbacksRegister(answers);
    cliSurface(answers);
  }

  /**
   * R2 to R12: what `print-config` prints, for the two roles the source ships as two programs.
   *
   * <p>The four entries naming a directory of the machine the run happened on are left out and
   * named here rather than quietly dropped: they answer with this machine's own paths, and the
   * two sides ran on different ones.
   */
  private static void configurationValues(ObjectNode answers) throws Exception {
    java.util.Set<String> machineSpecific =
        java.util.Set.of(
            "AUTH_JWKS_STATIC_DIR", "POLICY_REPO_CLONE_PATH", "BASE_DIR", "GIT_SSH_KEY_FILE");
    Map<String, String> empty = new HashMap<>();
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    rows.add(printed("server", new ServerConfig(empty), new CommonConfig(empty), machineSpecific));
    CommonConfig commonForClient = new CommonConfig(empty);
    ClientConfig clientConfig = new ClientConfig(empty);
    clientConfig.onLoad(commonForClient);
    rows.add(printed("client", clientConfig, commonForClient, machineSpecific));
    answers.set("configuration-values", rows);
  }

  private static ObjectNode printed(
      String label,
      io.akka.opal.common.confi.Confi own,
      CommonConfig common,
      java.util.Set<String> skip)
      throws Exception {
    ObjectNode row = Rpc.MAPPER.createObjectNode();
    row.put("set", label);
    TreeMap<String, String> values = new TreeMap<>();
    values.putAll(readPrinted(common));
    values.putAll(readPrinted(own));
    skip.forEach(values::remove);
    row.set("printed", Rpc.MAPPER.valueToTree(values));
    return row;
  }

  private static Map<String, String> readPrinted(io.akka.opal.common.confi.Confi config)
      throws Exception {
    return Rpc.MAPPER.readValue(
        config.printConfig(),
        Rpc.MAPPER.getTypeFactory().constructMapType(TreeMap.class, String.class, String.class));
  }

  /** R17 to R21: directories become topics, and the descendants drop out first. */
  private static void topicDirectories(Map<String, JsonNode> workloads, ObjectNode answers) {
    JsonNode workload = workloads.get("topic-directories");
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (JsonNode set : workload.get("sets")) {
      List<String> directories = strings(set);
      ObjectNode row = rows.addObject();
      row.set("directories", Rpc.MAPPER.valueToTree(directories));
      row.set("topics", Rpc.MAPPER.valueToTree(
          new TreeSet<>(Topics.pubsubTopicsFromDirectories(directories))));
      row.set("policyTopics", Rpc.MAPPER.valueToTree(
          new TreeSet<>(Topics.policyTopics(directories))));
    }
    for (JsonNode paths : workload.get("paths")) {
      ObjectNode row = rows.addObject();
      row.set("paths", Rpc.MAPPER.valueToTree(strings(paths)));
      row.set("intermediate", Rpc.MAPPER.valueToTree(
          new TreeSet<>(Paths2.intermediateDirectories(strings(paths)))));
    }
    ObjectNode last = rows.addObject();
    ObjectNode removePrefix = last.putObject("removePrefix");
    removePrefix.put("policy:a/b", Topics.removePrefix("policy:a/b"));
    removePrefix.put("a/b", Topics.removePrefix("a/b"));
    removePrefix.put("policy:", Topics.removePrefix("policy:"));
    answers.set("topic-directories", rows);
  }

  /** R38, R39: what two commits announce, and what the same commit twice announces. */
  private static void policyUpdateNotifications(
      ObjectNode answers, Repository repository, ObjectId first, ObjectId second) throws Exception {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (String[] step : new String[][] {{"first-to-second"}, {"same-commit"}}) {
      ObjectId left = step[0].equals("same-commit") ? second : first;
      Policy.PolicyUpdateMessageNotification notification =
          PolicyUpdates.createPolicyUpdate(
              repository, left, second, List.of(".rego", ".json"), List.of());
      ObjectNode row = rows.addObject();
      row.put("step", step[0]);
      row.set("topics", Rpc.MAPPER.valueToTree(new TreeSet<>(notification.topics())));
      row.set(
          "changedDirectories",
          Rpc.MAPPER.valueToTree(new TreeSet<>(notification.update().changed_directories())));
    }
    answers.set("policy-update-notifications", rows);
  }

  /** R52: a string is hashed as itself, everything else as its compact JSON. */
  private static void reportHashes(Map<String, JsonNode> workloads, ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    for (JsonNode payload : workloads.get("report-hashes").get("payloads")) {
      ObjectNode row = rows.addObject();
      row.set("payload", payload);
      row.put("hash", Hashing.calcHash(payload));
    }
    answers.set("report-hashes", rows);
  }

  /** R68, R70, R71: a token round trip, and the three refusals. */
  private static void jwtRoundTrip(ObjectNode answers) throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair pair = generator.generateKeyPair();
    JwtSigner signer =
        new JwtSigner(
            pem("PRIVATE KEY", pair.getPrivate().getEncoded()),
            pem("PUBLIC KEY", pair.getPublic().getEncoded()),
            EncryptionKeyFormat.pem,
            EncryptionKeyFormat.pem,
            null,
            JWTAlgorithm.RS256,
            "https://api.opal.ac/v1/",
            "https://opal.ac/");

    String token =
        signer.sign(
            java.util.UUID.randomUUID().toString().replace("-", ""),
            Duration.ofHours(1),
            Map.of("peer_type", "client"));
    Map<String, Object> claims = signer.verify(token);

    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    ObjectNode ok = rows.addObject();
    ok.put("step", "signed-and-verified");
    ok.set("claims", Rpc.MAPPER.valueToTree(new TreeSet<>(claims.keySet())));
    ok.put("peerType", String.valueOf(claims.get("peer_type")));
    ok.put("audience", String.valueOf(claims.get("aud")));
    ok.put("issuer", String.valueOf(claims.get("iss")));

    for (String[] bad :
        new String[][] {
          {"garbage", "not-a-token"}, {"empty", ""}, {"truncated", token.substring(0, token.length() - 4)}
        }) {
      ObjectNode row = rows.addObject();
      row.put("step", bad[0]);
      try {
        signer.verify(bad[1]);
        row.putNull("refused");
      } catch (Unauthorized e) {
        row.put("refused", e.getMessage());
      }
    }
    answers.set("jwt-round-trip", rows);
  }

  private static String pem(String type, byte[] der) throws Exception {
    StringWriter out = new StringWriter();
    try (PemWriter writer = new PemWriter(out)) {
      writer.writeObject(new PemObject(type, der));
    }
    return out.toString();
  }

  /** R85: the six constructions, and what each answers. */
  private static void storeAuthModes(ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    add(rows, "none", () -> store(PolicyStoreAuth.NONE, null, null, null, null, null, null, null));
    add(rows, "token-ok", () -> store(PolicyStoreAuth.TOKEN, "t", null, null, null, null, null, null));
    add(rows, "token-missing", () -> store(PolicyStoreAuth.TOKEN, null, null, null, null, null, null, null));
    add(rows, "oauth-ok",
        () -> store(PolicyStoreAuth.OAUTH, null, "i", "s", "http://x", null, null, null));
    add(rows, "oauth-missing", () -> store(PolicyStoreAuth.OAUTH, null, null, null, null, null, null, null));
    add(rows, "tls-missing", () -> store(PolicyStoreAuth.TLS, null, null, null, null, null, null, null));
    answers.set("store-auth-modes", rows);
  }

  private static void add(ArrayNode rows, String mode, Runnable build) {
    ObjectNode row = rows.addObject();
    row.put("mode", mode);
    try {
      build.run();
      row.put("outcome", "ok");
    } catch (RuntimeException e) {
      // The source raises a bare Exception and prints it as `Exception: <message>`; the shape
      // compared is the sentence, which is what an operator reads in the log.
      row.put("outcome", "Exception: " + e.getMessage());
    }
  }

  private static OpaClient store(
      PolicyStoreAuth auth,
      String token,
      String clientId,
      String clientSecret,
      String oauthServer,
      String cert,
      String key,
      String ca) {
    return new OpaClient(
        "http://127.0.0.1:1", token, auth, clientId, clientSecret, oauthServer, true, true, false,
        cert, key, ca, List.of(), "engine/healthcheck/opal.rego", ConnRetryOptions.defaults());
  }

  /** R90: the transaction log as a Rego module, compared as the whole text. */
  private static void healthPolicy(ObjectNode answers) {
    String template =
        "package system.opal.transactions\n\n"
            + "default ready = false\nready = {ready}\n"
            + "default healthy = false\nhealthy = {healthy}\n"
            + "last_policy_transaction = {last_policy_transaction}\n"
            + "last_data_transaction = {last_data_transaction}\n"
            + "last_failed_policy_transaction = {last_failed_policy_transaction}\n"
            + "last_failed_data_transaction = {last_failed_data_transaction}\n"
            + "transaction_data_statistics = {transaction_data_statistics}\n"
            + "transaction_policy_statistics = {transaction_policy_statistics}\n";

    TransactionLogState state = new TransactionLogState(true, true);
    state.processTransaction(
        new Store.StoreTransaction(
            "1", List.of("set_policies"), Store.TransactionType.policy, true, "", "t0", "t1", null));

    List<String[]> written = new ArrayList<>();
    TransactionLogPolicyWriter writer =
        new TransactionLogPolicyWriter(
            new CapturingStore(written), "system/opal/transactions", template);
    writer.persist(state);

    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    ObjectNode row = rows.addObject();
    row.put("id", written.get(0)[0]);
    row.put("code", written.get(0)[1]);
    answers.set("health-policy", rows);
  }

  private static final class CapturingStore
      extends io.akka.opal.client.store.StubPolicyStore {
    private final List<String[]> written;

    CapturingStore(List<String[]> written) {
      this.written = written;
    }

    @Override
    public void setPolicy(String policyId, String policyCode, String transactionId) {
      written.add(new String[] {policyId, policyCode});
    }
  }

  /** R97: the automatic key, and what an explicit one for the same callback does to it. */
  private static void callbacksRegister(ObjectNode answers) {
    CallbacksRegister register =
        new CallbacksRegister(List.of(), Data.HttpFetcherConfig.defaultCallbackConfig());
    Data.HttpFetcherConfig config =
        new Data.HttpFetcherConfig(null, null, null, null, Data.HttpMethods.POST, null);

    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    String automatic = register.put("http://cb", config, null);
    ObjectNode firstRow = rows.addObject();
    firstRow.put("step", "automatic");
    firstRow.put("key", automatic);
    firstRow.put("size", register.all().size());

    String explicit = register.put("http://cb", config, "mine");
    ObjectNode secondRow = rows.addObject();
    secondRow.put("step", "explicit-same-callback");
    secondRow.put("key", explicit);
    secondRow.put("size", register.all().size());
    secondRow.put("automaticStillThere", register.get(automatic) != null);

    answers.set("callbacks-register", rows);
  }

  /** R134 to R138: the commands, the secret formats, the version and the empty publish. */
  private static void cliSurface(ObjectNode answers) {
    ArrayNode rows = Rpc.MAPPER.createArrayNode();
    List<String> known =
        List.of("run", "print-config", "obtain-token", "generate-secret", "publish-data-update",
            "version");
    for (OpalCli.Which which : OpalCli.Which.values()) {
      String help = capture(which, new String[] {});
      TreeSet<String> found = new TreeSet<>();
      for (String line : help.split("\\R")) {
        String trimmed = line.trim();
        if (line.startsWith("  ") && known.contains(trimmed)) {
          found.add(trimmed);
        }
      }
      ObjectNode row = rows.addObject();
      row.put("module", "opal_" + which.name() + ".cli");
      row.set("commands", Rpc.MAPPER.valueToTree(found));
    }
    ObjectNode secrets = rows.addObject();
    ObjectNode lengths = secrets.putObject("generateSecret");
    lengths.put(
        "urlsafeLength",
        capture(OpalCli.Which.server, new String[] {"generate-secret", "--size", "8"}).trim()
            .length());
    lengths.put(
        "hexLength",
        capture(
                OpalCli.Which.server,
                new String[] {"generate-secret", "--size", "8", "--format", "hex"})
            .trim()
            .length());

    rows.addObject().put("version", capture(OpalCli.Which.server, new String[] {"version"}).trim());
    rows.addObject()
        .put(
            "publishNoArguments",
            capture(OpalCli.Which.server, new String[] {"publish-data-update"}).split("\\R")[0]
                .trim());
    answers.set("cli-surface", rows);
  }

  private static String capture(OpalCli.Which which, String[] arguments) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new OpalCli(which, Map.of(), new PrintStream(out, true, StandardCharsets.UTF_8)).run(arguments);
    return out.toString(StandardCharsets.UTF_8);
  }

  private static List<String> strings(JsonNode node) {
    List<String> out = new ArrayList<>();
    node.forEach(item -> out.add(item.asText()));
    return out;
  }
}
