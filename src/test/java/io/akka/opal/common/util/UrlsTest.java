package io.akka.opal.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Redaction and query-parameter setting, against the eleven URLs and three rewrites the source
 * produced.
 *
 * <p>These are checked against the source rather than against a description because they are what
 * stands between a credential in a data-source URL and a log file: getting the mask right for
 * eight of eleven shapes is the same as getting it wrong.
 */
class UrlsTest {

  @Test
  void redactionMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("redact_url");
    for (Iterator<Map.Entry<String, JsonNode>> it = recorded.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> row = it.next();
      assertEquals(row.getValue().asText(), Urls.redactUrl(row.getKey()), row.getKey());
    }
  }

  @Test
  void queryParameterSettingMatchesTheSource() {
    for (JsonNode row : SourceAnswers.get("set_url_query_param")) {
      assertEquals(
          row.get("output").asText(),
          Urls.setUrlQueryParam(
              row.get("url").asText(), row.get("name").asText(), row.get("value").asText()),
          row.get("url").asText());
    }
  }

  /** The known URL is replaced first, then any other credentials in the text are scrubbed. */
  @Test
  void freeTextIsScrubbedAroundAKnownUrl() {
    String url = "https://user:pw@github.com/o/r.git";
    String text = "fatal: could not read from " + url + " and https://a:b@other/x";
    String scrubbed = Urls.redactUrlInText(text, url);
    assertEquals(
        "fatal: could not read from https://***@github.com/o/r.git and https://***@other/x",
        scrubbed);
  }

  @Test
  void redactionNeverThrows() {
    assertEquals("not a url", Urls.redactUrl("not a url"));
    assertEquals("", Urls.redactUrl(""));
    assertEquals(null, Urls.redactUrl(null));
  }
}
