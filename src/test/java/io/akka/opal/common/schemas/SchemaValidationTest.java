package io.akka.opal.common.schemas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R142 to R144 — the three schemas that refuse a value, against the source's own answers.
 *
 * <p>Each of the three is a class of inputs rather than one input, and the class was enumerated by
 * the probe that recorded these answers: five ways to build a data source entry, five patch
 * actions, four combinations of the server's two mutually exclusive fields. What makes them worth
 * checking separately from the routes that carry them is that a refusal here is the difference
 * between a caller learning its update is malformed and a policy engine being written with a
 * document nobody can read back.
 */
class SchemaValidationTest {

  /** R142, over the five entries the source's probe built. */
  @Test
  void dataSourceEntryValidationMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("data_source_entry_validation");

    assertTrue(recorded.get("plain").get("ok").asBoolean(), "the source accepted a bare entry");
    Data.DataSourceEntry plain =
        new Data.DataSourceEntry("http://a", null, null, null, null, null, null);
    JsonNode plainValue = recorded.get("plain").get("value");
    assertEquals(plainValue.get("save_method").asText(), plain.save_method());
    assertEquals(plainValue.get("dst_path").asText(), plain.dst_path());
    assertEquals(SourceAnswers.strings(plainValue.get("topics")), plain.topics());

    assertTrue(recorded.get("put_with_dict").get("ok").asBoolean());
    Data.DataSourceEntry putWithObject =
        new Data.DataSourceEntry("http://a", null, null, null, "PUT", Map.of("x", 1), null);
    assertEquals("PUT", putWithObject.save_method());

    assertTrue(recorded.get("patch_with_list").get("ok").asBoolean());
    Data.DataSourceEntry patchWithList =
        new Data.DataSourceEntry(
            "http://a",
            null,
            null,
            null,
            "PATCH",
            List.of(new Store.JSONPatchAction("add", "/x", 1, null)),
            null);
    assertEquals("PATCH", patchWithList.save_method());

    assertEquals("ValidationError", recorded.get("patch_with_dict").get("error").asText());
    assertThrows(
        Schemas.ValidationFailure.class,
        () ->
            new Data.DataSourceEntry(
                "http://a", null, null, null, "PATCH", Map.of("x", 1), null));

    assertEquals("ValidationError", recorded.get("bad_save_method").get("error").asText());
    assertThrows(
        Schemas.ValidationFailure.class,
        () ->
            new Data.DataSourceEntry(
                "http://a", null, null, null, "DELETE", Map.of("x", 1), null));
  }

  /**
   * The half of R142 that is easy to get backwards: the source hangs its check on the {@code data}
   * field, so an entry carrying no data is accepted whatever its save method says.
   */
  @Test
  void anEntryWithNoDataIsNotCheckedForItsSaveMethod() {
    Data.DataSourceEntry entry =
        new Data.DataSourceEntry("http://a", null, null, null, "DELETE", null, null);
    assertEquals("DELETE", entry.save_method());
  }

  /** R143, over the five actions the source's probe built. */
  @Test
  void jsonPatchActionValidationMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("json_patch_validation");

    assertTrue(recorded.get("add_with_value").get("ok").asBoolean());
    assertEquals("add", new Store.JSONPatchAction("add", "/a", 1, null).op());

    assertTrue(recorded.get("remove_no_value").get("ok").asBoolean());
    assertEquals("remove", new Store.JSONPatchAction("remove", "/a", null, null).op());

    assertTrue(recorded.get("move_with_from").get("ok").asBoolean());
    assertEquals("/b", new Store.JSONPatchAction("move", "/a", null, "/b").from());

    assertEquals("ValidationError", recorded.get("add_no_value").get("error").asText());
    assertThrows(
        Schemas.ValidationFailure.class, () -> new Store.JSONPatchAction("add", "/a", null, null));

    assertEquals("ValidationError", recorded.get("replace_no_value").get("error").asText());
    assertThrows(
        Schemas.ValidationFailure.class,
        () -> new Store.JSONPatchAction("replace", "/a", null, null));
  }

  /** R144, over the four combinations of the server's two fields. */
  @Test
  void serverDataSourceConfigValidationMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("server_data_source_config_validation");

    assertEquals("ok", recorded.get("config_only").asText());
    assertEquals(
        List.of(),
        new Data.ServerDataSourceConfig(new Data.DataSourceConfig(List.of()), null)
            .config()
            .entries());

    assertEquals("ok", recorded.get("url_only").asText());
    assertEquals(
        "https://x.com/c",
        new Data.ServerDataSourceConfig(null, "https://x.com/c").external_source_url());

    assertEquals("ValidationError", recorded.get("neither").asText());
    assertThrows(
        Schemas.ValidationFailure.class, () -> new Data.ServerDataSourceConfig(null, null));

    assertEquals("ValidationError", recorded.get("both").asText());
    assertThrows(
        Schemas.ValidationFailure.class,
        () ->
            new Data.ServerDataSourceConfig(
                new Data.DataSourceConfig(List.of()), "https://x.com/c"));
  }
}
