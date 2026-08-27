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

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(DataFetcher.class);

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
   * R263: one entry's value — inline, fetched, or refused.
   *
   * <p>An entry with neither inline data nor a url of its own is refused rather than sent to the
   * configured default: the default belongs to a caller that asked for no urls at all, and an
   * entry that named nowhere is a publication nobody can act on. Its report then says it fetched
   * nothing, which is the answer.
   */
  public JsonNode handleUrl(String url, Map<String, Object> config, Object inlineData) {
    if (inlineData != null) {
      log.info("Data provided inline for url: {}", io.akka.opal.common.util.Urls.redactUrl(url));
      return Rpc.MAPPER.valueToTree(inlineData);
    }
    if (url == null || url.isEmpty()) {
      log.error("Invalid data update: no embedded data or URL");
      return null;
    }
    log.info("Fetching data from url: {}", io.akka.opal.common.util.Urls.redactUrl(url));
    try {
      return engine.handleUrl(url, config, null);
    } catch (RuntimeException e) {
      // R339: a timeout is named as one. Every other failure carries its own message; this one
      // arrives as a bare interruption of the wait and reads like nothing happened.
      if (isTimeout(e)) {
        log.error("Timeout while fetching url: {}", io.akka.opal.common.util.Urls.redactUrl(url), e);
      }
      throw e;
    }
  }

  private static boolean isTimeout(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof java.util.concurrent.TimeoutException
          || cause instanceof java.net.http.HttpTimeoutException) {
        return true;
      }
      if (cause.getCause() == cause) {
        return false;
      }
    }
    return false;
  }

  /**
   * R340: every request in one go, and the built-in route when the caller named none.
   *
   * <p>The default carries the client's own token, because the route it names is the server's and
   * the server refuses an unauthenticated read. Nothing inside OPAL calls this without a list —
   * the callbacks reporter always has one — so the default is the shape of an offer rather than a
   * path anything takes.
   */
  public java.util.List<Fetched> handleUrls(java.util.List<Request> requests) {
    java.util.List<Request> effective =
        requests == null
            ? java.util.List.of(new Request(defaultDataUrl, defaultFetcherConfig(), null))
            : requests;
    java.util.List<java.util.concurrent.CompletableFuture<Fetched>> running =
        new java.util.ArrayList<>();
    for (Request request : effective) {
      running.add(
          java.util.concurrent.CompletableFuture.supplyAsync(
              () -> {
                try {
                  return new Fetched(
                      request.url(),
                      request.config(),
                      handleUrl(request.url(), request.config(), request.data()));
                } catch (RuntimeException e) {
                  return new Fetched(request.url(), request.config(), null);
                }
              }));
    }
    java.util.List<Fetched> results = new java.util.ArrayList<>();
    for (java.util.concurrent.CompletableFuture<Fetched> future : running) {
      Fetched fetched = future.join();
      // The source drops a result that came back empty rather than reporting it against its url.
      if (fetched.value() != null) {
        results.add(fetched);
      }
    }
    return results;
  }

  /** One thing to fetch: where from, how, and the value if the caller already has it. */
  public record Request(String url, Map<String, Object> config, Object data) {}

  /** What came back, against the request that asked for it. */
  public record Fetched(String url, Map<String, Object> config, JsonNode value) {}

  private Map<String, Object> defaultFetcherConfig() {
    Map<String, Object> config = new LinkedHashMap<>();
    if (token != null && !token.isEmpty()) {
      config.put("headers", Map.of("Authorization", "Bearer " + token));
    }
    config.put("is_json", true);
    return config;
  }

  /** Where a fetch goes when the caller named nowhere at all. */
  private String defaultDataUrl;

  /** The client's own token, attached to that default fetch. */
  private String token;

  public DataFetcher withDefaultDataUrl(String url) {
    this.defaultDataUrl = url;
    return this;
  }

  public DataFetcher withToken(String token) {
    this.token = token;
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
