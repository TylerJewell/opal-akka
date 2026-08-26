package io.akka.opal.client.data;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.fetcher.FetchingEngine;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.server.pubsub.Rpc;
import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fetching a data entry's value — SPEC-002 R48.
 *
 * <p>Inline {@code data} on the entry is used as-is and nothing is fetched, which is what lets a
 * publisher push a value rather than a place to read one from. An entry with neither inline data
 * nor a URL yields nothing at all, and the entry's report says so.
 *
 * <p>Everything else goes through the fetching engine, so an entry whose configuration names a
 * provider a deployment registered reaches that provider rather than the built-in one.
 */
public class DataFetcher implements AutoCloseable {

  private final FetchingEngine engine;

  public DataFetcher(HttpClient http, double timeoutSeconds) {
    this(new FetchingEngine(http, timeoutSeconds));
  }

  public DataFetcher(FetchingEngine engine) {
    this.engine = engine;
  }

  public FetchingEngine engine() {
    return engine;
  }

  /**
   * R263: an entry with neither inline data nor a url of its own reads the configured default.
   *
   * <p>{@code DEFAULT_DATA_URL} is what a deployment sets when every entry comes from one place
   * and the entries would rather not repeat it. Without it such an entry yields nothing and its
   * report says it fetched nothing, which reads like a failure of the source rather than of the
   * configuration.
   */
  public JsonNode handleUrl(String url, Map<String, Object> config, Object inlineData) {
    if (inlineData != null) {
      return Rpc.MAPPER.valueToTree(inlineData);
    }
    String target = url == null || url.isEmpty() ? defaultDataUrl : url;
    if (target == null || target.isEmpty()) {
      return null;
    }
    return engine.handleUrl(target, config, null);
  }

  /** Where a fetch goes when the entry names nowhere. */
  private String defaultDataUrl;

  public DataFetcher withDefaultDataUrl(String url) {
    this.defaultDataUrl = url;
    return this;
  }

  /** A single fetch described by a typed configuration, which the callbacks reporter uses. */
  public JsonNode fetch(String url, Data.HttpFetcherConfig config) {
    Map<String, Object> asMap =
        config == null
            ? null
            : Rpc.MAPPER.convertValue(
                config,
                Rpc.MAPPER
                    .getTypeFactory()
                    .constructMapType(LinkedHashMap.class, String.class, Object.class));
    return engine.handleUrl(url, asMap, null);
  }

  @Override
  public void close() {
    engine.close();
  }
}
