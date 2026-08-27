package io.akka.opal.common.fetcher;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.util.Http;
import io.akka.opal.common.util.Urls;
import io.akka.opal.server.pubsub.Rpc;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one provider OPAL ships — SPEC-002 R147.
 *
 * <p>Three switches on the configuration decide what a caller gets back, and they are not the
 * same switch: {@code is_json} decides whether the body is parsed, and {@code process_data}
 * decides whether the body is read at all. A caller that turns the second off is asking for the
 * response — status, headers and all — rather than for what is in it.
 */
public final class HttpFetchProvider implements FetchProvider {

  private static final Logger log = LoggerFactory.getLogger(HttpFetchProvider.class);

  private final FetchEvent event;
  private final Data.HttpFetcherConfig config;
  private final HttpClient http;
  private final Duration timeout;

  public HttpFetchProvider(FetchEvent event, HttpClient http, double timeoutSeconds) {
    this.event = event;
    this.config =
        event.config() == null
            ? Data.HttpFetcherConfig.defaults()
            : Rpc.MAPPER.convertValue(event.config(), Data.HttpFetcherConfig.class);
    this.http = http == null ? Http.forClient() : http;
    this.timeout = Duration.ofMillis((long) (timeoutSeconds * 1000));
  }

  /** What the source hands its callers when {@code process_data} is off. */
  public record RawResponse(int status, String body) {}

  @Override
  public Object fetch() {
    log.debug("HttpFetchProvider fetching from {}", Urls.redactUrl(event.url()));
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(event.url())).timeout(timeout);
    if (config.headers() != null) {
      config.headers().forEach(builder::header);
    }
    String method = config.methodOrGet().toUpperCase(Locale.ROOT);
    String body = bodyOf(config);
    if (body == null) {
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    }
    try {
      HttpResponse<String> response =
          io.akka.opal.common.util.Http.send(
              http, builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      if (response.statusCode() >= 400) {
        throw new IllegalStateException(
            "Failed to decode response from url: '"
                + Urls.redactUrl(event.url())
                + "', got response code "
                + response.statusCode()
                + " with response: "
                + response.body());
      }
      return new RawResponse(response.statusCode(), response.body());
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to fetch data for entry "
              + Urls.redactUrl(event.url())
              + ": "
              + Urls.redactUrlInText(e.toString(), event.url()),
          e);
    }
  }

  @Override
  public JsonNode process(Object raw) {
    RawResponse response = (RawResponse) raw;
    if (Boolean.FALSE.equals(config.process_data())) {
      // The response object itself, described rather than read: a caller that asked not to have
      // its data processed still needs something it can hand on.
      return Rpc.MAPPER
          .createObjectNode()
          .put("status", response.status())
          .put("text", response.body());
    }
    if (Boolean.FALSE.equals(config.is_json())) {
      return Rpc.MAPPER.getNodeFactory().textNode(response.body());
    }
    try {
      return Rpc.MAPPER.readTree(response.body());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to decode response from url: '" + Urls.redactUrl(event.url()) + "'", e);
    }
  }

  private static String bodyOf(Data.HttpFetcherConfig config) {
    if (config.data() == null) {
      return null;
    }
    if (config.data() instanceof String text) {
      return text;
    }
    try {
      return Rpc.MAPPER.writeValueAsString(config.data());
    } catch (Exception e) {
      return String.valueOf(config.data());
    }
  }
}
