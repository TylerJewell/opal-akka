package io.akka.opal.client.callbacks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-002 R97, against the keys and normalisations the source's own register produced. */
class CallbacksTest {

  private static Data.HttpFetcherConfig postJson() {
    return new Data.HttpFetcherConfig(
        null, Map.of("content-type", "application/json"), null, null, Data.HttpMethods.POST, null);
  }

  private static CallbacksRegister register(List<Object> initial) {
    return new CallbacksRegister(initial, Data.HttpFetcherConfig.defaultCallbackConfig());
  }

  /** R97: the key is a digest of the URL and the configuration, and it is stable. */
  @Test
  void keysMatchTheSource() {
    JsonNode recorded = SourceAnswers.get("callbacks_register");
    CallbacksRegister register = register(List.of());
    String key = register.calcHash("http://a/cb", postJson());
    assertEquals(recorded.get("auto_key").asText(), key);
    assertEquals(key, register.calcHash("http://a/cb", postJson()));
    assertNotEquals(
        key,
        register.calcHash(
            "http://a/cb",
            new Data.HttpFetcherConfig(null, null, null, null, Data.HttpMethods.GET, null)));
  }

  /** R97: the named registration removes the automatically keyed one. */
  @Test
  void anExplicitKeyReplacesTheAutomaticOne() {
    JsonNode recorded = SourceAnswers.get("callbacks_register");
    CallbacksRegister register = register(List.of());
    String auto = register.put("http://a/cb", postJson(), null);
    assertEquals(recorded.get("auto_key").asText(), auto);

    String named = register.put("http://a/cb", postJson(), "mykey");
    assertEquals(recorded.get("named_key").asText(), named);

    List<String> keys = new ArrayList<>();
    register.all().forEach(entry -> keys.add(entry.key()));
    java.util.Collections.sort(keys);
    assertEquals(SourceAnswers.strings(recorded.get("keys_after_named_put")), keys);
    assertNull(register.get(auto), "the automatically keyed entry is gone");
  }

  /** A register built from a list of URLs holds those URLs. */
  @Test
  void initialCallbacksMatchTheSource() {
    CallbacksRegister register = register(List.of("http://x/cb"));
    List<String> urls = new ArrayList<>();
    register.all().forEach(entry -> urls.add(entry.url()));
    assertEquals(SourceAnswers.strings(SourceAnswers.get("callbacks_register_initial")), urls);
  }

  /** A bare URL takes the default configuration; a pair carries its own. */
  @Test
  void normalisationMatchesTheSource() throws Exception {
    CallbacksRegister register = register(List.of("http://x/cb"));
    JsonNode recorded = SourceAnswers.get("callbacks_normalize");

    JsonNode pair =
        Rpc.MAPPER.readTree("[\"http://z/cb\", {\"method\": \"put\"}]");
    List<CallbacksRegister.CallbackConfig> normalized =
        register.normalizeCallbacks(List.of("http://y/cb", pair));

    assertEquals(recorded.get(0).get(0).asText(), normalized.get(0).url());
    assertEquals(
        recorded.get(0).get(1).get("method").asText(), normalized.get(0).config().methodOrGet());
    assertEquals(
        recorded.get(0).get(1).get("process_data").asBoolean(),
        normalized.get(0).config().process_data());

    assertEquals(recorded.get(1).get(0).asText(), normalized.get(1).url());
    assertEquals(
        recorded.get(1).get(1).get("method").asText(), normalized.get(1).config().methodOrGet());
    assertTrue(normalized.get(1).config().headers() == null);
  }

  /** An unsupported entry is dropped with a warning rather than failing the whole update. */
  @Test
  void anUnsupportedEntryIsDropped() {
    CallbacksRegister register = register(List.of());
    assertEquals(0, register.normalizeCallbacks(List.of(42)).size());
  }
}
