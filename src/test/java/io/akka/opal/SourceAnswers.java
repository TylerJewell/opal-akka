package io.akka.opal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;

/**
 * The answers the original gave when its own code was run, recorded by the probes under
 * {@code opal-port/probes/complete/} and carried here so the rebuild's tests compare against the
 * source rather than against a transcription of it.
 */
public final class SourceAnswers {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private static final JsonNode ONE = load("/source/answers.json");
  private static final JsonNode TWO = load("/source/answers-2.json");

  /** What the four live runs against the running original recorded, route by route. */
  public static final JsonNode LIVE_SERVER = load("/source/live-server.json");

  public static final JsonNode LIVE_CLIENT = load("/source/live-client.json");

  public static final JsonNode LIVE_SECURE = load("/source/live-secure.json");

  public static final JsonNode LIVE_SCOPES = load("/source/live-scopes.json");

  private SourceAnswers() {}

  private static JsonNode load(String resource) {
    try (InputStream in = SourceAnswers.class.getResourceAsStream(resource)) {
      return MAPPER.readTree(in);
    } catch (Exception e) {
      throw new IllegalStateException("could not read " + resource, e);
    }
  }

  public static JsonNode get(String key) {
    JsonNode node = ONE.get(key);
    for (JsonNode source : new JsonNode[] {TWO, LIVE_SERVER, LIVE_CLIENT, LIVE_SECURE, LIVE_SCOPES}) {
      if (node != null) {
        break;
      }
      node = source.get(key);
    }
    if (node == null) {
      throw new IllegalArgumentException("no recorded answer named " + key);
    }
    return node;
  }

  /** The node as compact JSON, for comparing a whole structure in one assertion. */
  public static String json(Object value) {
    try {
      return MAPPER.writeValueAsString(MAPPER.valueToTree(value));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String json(JsonNode node) {
    return node.toString();
  }

  /** A JSON array as a list of strings, which is the shape most recorded answers take. */
  public static java.util.List<String> strings(JsonNode node) {
    java.util.List<String> out = new java.util.ArrayList<>();
    node.forEach(item -> out.add(item.asText()));
    return out;
  }
}
