package io.akka.opal.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.client.engine.EngineLogLine;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.config.Enums.EngineLogFormat;
import io.akka.opal.common.git.ClonePathFinder;
import io.akka.opal.common.git.PolicyUpdates;
import io.akka.opal.common.util.Paths2;
import io.akka.opal.common.util.Urls;
import io.akka.opal.common.util.Version;
import io.akka.opal.server.LoadLimiter;
import io.akka.opal.server.pubsub.Rpc;
import io.akka.opal.server.webhook.Webhooks;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The workloads the second walk over the original added, on the rebuild's side.
 *
 * <p>Each drives the rebuild's own code over the same inputs {@code fourth_batch.py} drives the
 * original's, so what the benchmark compares is two systems answering rather than one system
 * against a recording of the other.
 */
final class FourthBatch {

  private FourthBatch() {}

  static void run(Map<String, JsonNode> byName, ObjectNode answers) {
    configDebugRepr(answers);
    urlRedaction(answers, byName.get("url-redaction"));
    webhookUrlShapes(answers, byName.get("webhook-url-shapes"));
    scopePolicyFiles(answers, byName.get("scope-policy-files"));
    limitNotation(answers, byName.get("limit-notation"));
    subscriptionDirectories(answers, byName.get("subscription-directories"));
    packageVersion(answers);
    engineLogColour(answers, byName.get("engine-log-colour"));
    clonePathCandidates(answers, byName.get("clone-path-candidates"));
  }

  /** R271: the per-entry repr form, which is not the printed form. */
  private static void configDebugRepr(ObjectNode answers) {
    String[] lines = new CommonConfig(Map.of()).debugRepr().split("\n");
    List<String> head = new ArrayList<>();
    for (int index = 0; index < Math.min(12, lines.length); index++) {
      head.add(lines[index]);
    }
    answers.set("config-debug-repr", Rpc.MAPPER.valueToTree(head));
  }

  /** R277: what a redacted url looks like, including the shapes a strict parser refuses. */
  private static void urlRedaction(ObjectNode answers, JsonNode workload) {
    Map<String, String> redacted = new LinkedHashMap<>();
    for (JsonNode url : workload.get("urls")) {
      redacted.put(url.asText(), Urls.redactUrl(url.asText()));
    }
    answers.set("url-redaction", Rpc.MAPPER.valueToTree(redacted));
  }

  /** R277: which repository urls a webhook payload is taken to be about. */
  private static void webhookUrlShapes(ObjectNode answers, JsonNode workload) {
    List<String> tracked = strings(workload.get("tracked"));
    List<String> names = strings(workload.get("names"));
    Map<String, Object> matched = new LinkedHashMap<>();
    for (JsonNode url : workload.get("urls")) {
      matched.put(
          "by-url|" + url.asText(),
          Webhooks.isMatchingWebhookUrl(url.asText(), tracked, List.of()));
      matched.put(
          "by-name|" + url.asText(),
          Webhooks.isMatchingWebhookUrl(url.asText(), List.of(), names));
    }
    answers.set("webhook-url-shapes", Rpc.MAPPER.valueToTree(matched));
  }

  /** R278: which files a scope announces a change to. */
  private static void scopePolicyFiles(ObjectNode answers, JsonNode workload) {
    Map<String, Map<String, Boolean>> byExtensions = new LinkedHashMap<>();
    Map<String, List<String>> lists =
        Map.of("rego-and-json", List.of(".rego", ".json"), "rego-only", List.of(".rego"));
    for (String key : List.of("rego-and-json", "rego-only")) {
      Map<String, Boolean> answered = new LinkedHashMap<>();
      for (JsonNode path : workload.get("paths")) {
        answered.put(
            path.asText(), PolicyUpdates.isRegoSourceFile(path.asText(), lists.get(key)));
      }
      byExtensions.put(key, answered);
    }
    answers.set("scope-policy-files", Rpc.MAPPER.valueToTree(byExtensions));
  }

  /** R280: every notation the rate limiter accepts, and what it makes of it. */
  private static void limitNotation(ObjectNode answers, JsonNode workload) {
    Map<String, String> parsed = new LinkedHashMap<>();
    for (JsonNode notation : workload.get("notations")) {
      try {
        parsed.put(notation.asText(), new LoadLimiter(notation.asText()).canonicalNotation());
      } catch (RuntimeException e) {
        parsed.put(notation.asText(), "raised");
      }
    }
    answers.set("limit-notation", Rpc.MAPPER.valueToTree(parsed));
  }

  /** R293: the directory set a client reduces before it subscribes. */
  private static void subscriptionDirectories(ObjectNode answers, JsonNode workload) {
    Map<String, List<String>> reduced = new LinkedHashMap<>();
    JsonNode sets = workload.get("sets");
    java.util.Iterator<String> keys = sets.fieldNames();
    while (keys.hasNext()) {
      String key = keys.next();
      List<String> answered = new ArrayList<>(
          Paths2.nonIntersectingDirectories(strings(sets.get(key))));
      answered.sort(String::compareTo);
      reduced.put(key, answered);
    }
    answers.set("subscription-directories", Rpc.MAPPER.valueToTree(reduced));
  }

  /** R279: the version the packaging metadata reports. */
  private static void packageVersion(ObjectNode answers) {
    answers.put("package-version", Version.current());
  }

  /** R363: one line of engine output rendered with the log colourised, and without. */
  private static void engineLogColour(ObjectNode answers, JsonNode workload) {
    EngineLogFormat format = EngineLogFormat.valueOf(workload.get("format").asText().toUpperCase());
    Map<String, String> rendered = new LinkedHashMap<>();
    for (boolean colorize : new boolean[] {false, true}) {
      for (JsonNode line : workload.get("lines")) {
        EngineLogLine.Rendered answer = EngineLogLine.render(line.asText(), format, colorize);
        rendered.put(
            (colorize ? "colour" : "plain") + "|" + line.asText(),
            answer == null ? "" : answer.text());
      }
    }
    answers.set("engine-log-colour", Rpc.MAPPER.valueToTree(rendered));
  }

  /** R379: what the clone-path finder counts as a candidate. */
  private static void clonePathCandidates(ObjectNode answers, JsonNode workload) {
    Map<String, String> found = new LinkedHashMap<>();
    for (JsonNode testCase : workload.get("cases")) {
      try {
        Path base = Files.createTempDirectory("opal-clone-candidates-");
        for (JsonNode entry : testCase.get("entries")) {
          Path target = base.resolve(entry.get("name").asText());
          if ("directory".equals(entry.get("kind").asText())) {
            Files.createDirectories(target);
          } else {
            Files.writeString(target, "");
          }
        }
        Path chosen =
            new ClonePathFinder(base.toString(), workload.get("prefix").asText(), false)
                .clonePath();
        found.put(
            testCase.get("name").asText(),
            chosen == null ? null : chosen.getFileName().toString());
      } catch (Exception e) {
        found.put(testCase.get("name").asText(), "raised:" + e.getClass().getSimpleName());
      }
    }
    answers.set("clone-path-candidates", Rpc.MAPPER.valueToTree(found));
  }

  private static List<String> strings(JsonNode node) {
    List<String> values = new ArrayList<>();
    if (node != null) {
      for (JsonNode entry : node) {
        values.add(entry.asText());
      }
    }
    return values;
  }
}
