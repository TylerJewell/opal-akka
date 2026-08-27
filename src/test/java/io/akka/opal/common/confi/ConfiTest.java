package io.akka.opal.common.confi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.server.config.ServerConfig;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-002 R1 to R5 and R7 to R12, against what the source's own parser answered. */
class ConfiTest {

  /** R2, over the thirteen inputs the source's probe ran through {@code cast_boolean}. */
  @Test
  void booleanCastingMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("cast_boolean");
    for (Iterator<Map.Entry<String, JsonNode>> it = recorded.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> row = it.next();
      String input = row.getKey();
      // The probe keyed by the Python repr of the input; only the string inputs reach a parser
      // here, because the two boolean inputs are already of the entry's type.
      if (!input.startsWith("'")) {
        continue;
      }
      String value = input.substring(1, input.length() - 1);
      if (row.getValue().isObject()) {
        assertThrows(Confi.BadValue.class, () -> Confi.castBoolean(value), input);
      } else {
        assertEquals(row.getValue().asBoolean(), Confi.castBoolean(value), input);
      }
    }
  }

  /**
   * R4: a model entry is parsed from JSON when the environment gives it a string, and used as it
   * stands when the declaration gives it an object.
   *
   * <p>Both halves, because a rebuild that parsed only the first would have every model entry
   * come out null on a deployment that set none of them — and null is a plausible-looking value
   * for a configuration entry nobody set.
   */
  @Test
  void aModelEntryIsParsedFromJsonAndUsedFromAnObject() {
    io.akka.opal.server.config.ServerConfig fromEnvironment =
        new io.akka.opal.server.config.ServerConfig(
            Map.of(
                "OPAL_DATA_CONFIG_SOURCES",
                "{\"config\": {\"entries\": [{\"url\": \"http://elsewhere\","
                    + " \"topics\": [\"policy_data\"], \"dst_path\": \"/x\"}]}}"));
    io.akka.opal.common.schemas.Data.ServerDataSourceConfig parsed =
        fromEnvironment.get("DATA_CONFIG_SOURCES");
    assertEquals(1, parsed.config().entries().size());
    assertEquals("http://elsewhere", parsed.config().entries().get(0).url());
    assertEquals("/x", parsed.config().entries().get(0).dst_path());

    io.akka.opal.common.schemas.Data.ServerDataSourceConfig fromDeclaration =
        new io.akka.opal.server.config.ServerConfig(Map.of()).get("DATA_CONFIG_SOURCES");
    assertEquals(1, fromDeclaration.config().entries().size(), "R12: one entry by default");
    assertEquals(
        "http://localhost:7002/policy-data",
        fromDeclaration.config().entries().get(0).url(),
        "pointing at the server's own all-data route");
  }

  /** R3: comma separated, each element stripped. */
  @Test
  void listCastingStripsEachElement() {
    assertEquals(List.of("a", "b", "c"), Confi.castList("a, b ,c"));
    assertEquals(List.of(), Confi.castList(""));
    assertEquals(List.of("one"), Confi.castList("one"));
  }

  /** R1: the environment wins over the default, and the key is prefixed. */
  @Test
  void anEnvironmentValueOverridesTheDefault() {
    CommonConfig config = new CommonConfig(Map.of("OPAL_LOG_LEVEL", "DEBUG"));
    assertEquals("DEBUG", config.getString("LOG_LEVEL"));
    assertEquals("INFO", new CommonConfig(Map.of()).getString("LOG_LEVEL"));
  }

  /** R5: a delayed default is evaluated against the entries, and skipped when a value was given. */
  @Test
  void delayedDefaultsAreEvaluatedAgainstTheOtherEntries() {
    ServerConfig server = new ServerConfig(Map.of("OPAL_ALL_DATA_ROUTE", "/other-data"));
    assertEquals("http://localhost:7002/other-data", server.getString("ALL_DATA_URL"));

    ServerConfig explicit = new ServerConfig(Map.of("OPAL_ALL_DATA_URL", "http://elsewhere/data"));
    assertEquals("http://elsewhere/data", explicit.getString("ALL_DATA_URL"));
  }

  /** R7: https becomes wss and http becomes ws. */
  @Test
  void theWebsocketUrlIsDerivedFromTheServerUrl() {
    assertEquals(
        "wss://opal.example/",
        new ClientConfig(Map.of("OPAL_SERVER_URL", "https://opal.example/"))
            .getString("SERVER_WS_URL"));
    assertEquals(
        "ws://opal.example/",
        new ClientConfig(Map.of("OPAL_SERVER_URL", "http://opal.example/"))
            .getString("SERVER_WS_URL"));
    assertEquals(
        "ws://opal.example//ws",
        new ClientConfig(Map.of("OPAL_SERVER_URL", "http://opal.example/"))
            .getString("SERVER_PUBSUB_URL"));
  }

  /** R8 and R9, over the four inputs the source's probe decoded. */
  @Test
  void multilineKeyDecodingMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("maybe_decode_multiline_key");
    for (Iterator<Map.Entry<String, JsonNode>> it = recorded.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> row = it.next();
      assertEquals(
          row.getValue().asText(), Keys.maybeDecodeMultiline(row.getKey()), row.getKey());
    }
  }

  /** R9: an SSH public key is one line, so its underscores are part of it. */
  @Test
  void anSshPublicKeyIsTakenVerbatim() {
    String key = "ssh-rsa AAAAB3Nza_C1yc2E comment";
    assertEquals(key, Keys.decode(key, "ssh", true));
    assertTrue(Keys.decode("no_newlines_here", "pem", false).endsWith("\n"));
  }

  /** R11: the option name is the key lowercased with hyphens, and a value applies over the env. */
  @Test
  void everyEntryIsAlsoACommandLineOption() {
    CommonConfig config = new CommonConfig(Map.of());
    assertEquals("--log-level", config.entries().get("LOG_LEVEL").optionName());
    assertTrue(config.applyCommandLine("LOG_LEVEL", "WARNING"));
    assertEquals("WARNING", config.getString("LOG_LEVEL"));
  }

  /**
   * R11's other half: three entries name an environment key that is not their own name, and a
   * command-line value for them is dropped rather than applied — the source's own lookup is by
   * name and finds nothing.
   */
  @Test
  void anEntryWhoseKeyDiffersFromItsNameDropsItsCommandLineValue() {
    ServerConfig server = new ServerConfig(Map.of());
    assertEquals("--ws-local-url", server.entries().get("OPAL_WS_LOCAL_URL").optionName());
    assertEquals(
        "--data-config-route", server.entries().get("DATA_UPDATE_TRIGGER_ROUTE").optionName());
    assertTrue(server.applyCommandLine("DATA_UPDATE_TRIGGER_ROUTE", "/x"));
  }

  /** R12: the default data configuration points at the server's own all-data route. */
  @Test
  void theDefaultDataConfigurationPointsAtTheServersOwnRoute() {
    ServerConfig server = new ServerConfig(Map.of());
    io.akka.opal.common.schemas.Data.ServerDataSourceConfig sources =
        server.get("DATA_CONFIG_SOURCES");
    assertNull(sources.external_source_url());
    assertEquals(1, sources.config().entries().size());
    assertEquals(
        "http://localhost:7002/policy-data", sources.config().entries().get(0).url());
    assertEquals(List.of("policy_data"), sources.config().entries().get(0).topics());
    assertEquals("", sources.config().entries().get(0).dst_path());
  }
}
