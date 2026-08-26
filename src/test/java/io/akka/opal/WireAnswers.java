package io.akka.opal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * What the original put on a wire, or wrote to a log, when its own code was run.
 *
 * <p>Recorded by {@code opal-port/probes/complete/record_wire_answers.py}, which drives OPAL's own
 * statsd client under a bound socket, its engine log renderer under a capturing sink, its
 * policy-store client against a recording server, and its retry policy asked what it would wait.
 * Kept apart from {@link SourceAnswers} because those answers are return values and these are
 * bytes and lines — the two are recorded by different means and go stale for different reasons.
 */
public final class WireAnswers {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final JsonNode ALL = load("/source/wire-answers.json");

  private WireAnswers() {}

  private static JsonNode load(String resource) {
    try (InputStream in = WireAnswers.class.getResourceAsStream(resource)) {
      return MAPPER.readTree(in);
    } catch (Exception e) {
      throw new IllegalStateException("could not read " + resource, e);
    }
  }

  public static JsonNode get(String key) {
    JsonNode node = ALL.get(key);
    if (node == null) {
      throw new IllegalArgumentException("no recorded answer named " + key);
    }
    return node;
  }

  /** An array of strings as a list, which is most of what these answers are. */
  public static List<String> strings(JsonNode array) {
    List<String> out = new ArrayList<>();
    array.forEach(element -> out.add(element.asText()));
    return out;
  }
}
