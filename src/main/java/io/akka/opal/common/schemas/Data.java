package io.akka.opal.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/** The data half of OPAL's wire schema — SPEC-002 section 2.2. */
public final class Data {

  public static final String DEFAULT_DATA_TOPIC = "policy_data";

  private Data() {}

  /**
   * One data source and how to write what it returns. The publisher rewrites {@code topics} to
   * its own expansion before the entry goes on the wire (R22), so a receiver never expands a
   * second time.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record DataSourceEntry(
      String url,
      Map<String, Object> config,
      List<String> topics,
      String dst_path,
      String save_method,
      Object data,
      // The source keeps this on a subclass of its own, so an entry without one carries no such
      // key on the wire and an entry with one does.
      @JsonInclude(JsonInclude.Include.NON_NULL) Double periodic_update_interval)
      implements io.akka.opal.common.util.Repr.Reprable {

    public DataSourceEntry {
      if (topics == null) {
        topics = List.of(DEFAULT_DATA_TOPIC);
      }
      if (dst_path == null) {
        dst_path = "";
      }
      if (save_method == null) {
        save_method = "PUT";
      }
      // R142: the check hangs off `data`, so an entry that carries none is accepted whatever
      // its save_method says — an entry with nothing to write never reaches the branch the
      // method selects.
      if (data != null) {
        if (!"PUT".equals(save_method) && !"PATCH".equals(save_method)) {
          throw new Schemas.ValidationFailure("'save_method' must be either PUT or PATCH");
        }
        if ("PATCH".equals(save_method) && !isPatchDocument(data)) {
          throw new Schemas.ValidationFailure(
              "'data' must be of type JSON patch request when save_method is PATCH");
        }
      }
    }

    /** A list whose every element is, or reads as, one RFC-6902 action. */
    private static boolean isPatchDocument(Object data) {
      if (!(data instanceof List<?> items)) {
        return false;
      }
      for (Object item : items) {
        if (item instanceof Store.JSONPatchAction) {
          continue;
        }
        if (item instanceof Map<?, ?> fields
            && fields.get("op") instanceof String
            && fields.get("path") instanceof String) {
          continue;
        }
        return false;
      }
      return true;
    }

    public static DataSourceEntry of(String url, List<String> topics, String dstPath) {
      return new DataSourceEntry(url, null, topics, dstPath, "PUT", null, null);
    }

    public DataSourceEntry withTopics(List<String> newTopics) {
      return new DataSourceEntry(
          url, config, newTopics, dst_path, save_method, data, periodic_update_interval);
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "url=" + io.akka.opal.common.util.Repr.repr(url),
          "config='<redacted>'",
          "topics=" + io.akka.opal.common.util.Repr.repr(topics),
          "dst_path=" + io.akka.opal.common.util.Repr.repr(dst_path),
          "save_method=" + io.akka.opal.common.util.Repr.repr(save_method),
          "data='<redacted>'",
          "periodic_update_interval="
              + io.akka.opal.common.util.Repr.repr(periodic_update_interval));
    }

    /** A configuration entry holds the source's polling subclass, which names itself. */
    @Override
    public String pyClassName() {
      return "DataSourceEntryWithPollingInterval";
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record DataSourceConfig(List<DataSourceEntry> entries)
      implements io.akka.opal.common.util.Repr.Reprable {
    public DataSourceConfig {
      if (entries == null) {
        entries = List.of();
      }
    }

    @Override
    public List<String> pyFields() {
      return List.of("entries=" + io.akka.opal.common.util.Repr.repr(entries));
    }
  }

  /** R144: exactly one of the two is set; both, or neither, is a validation failure. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ServerDataSourceConfig(DataSourceConfig config, String external_source_url)
      implements io.akka.opal.common.util.Repr.Reprable {

    public ServerDataSourceConfig {
      if (external_source_url != null) {
        Schemas.requireHttpUrl(external_source_url, "external_source_url");
      }
      if (config == null && external_source_url == null) {
        throw new Schemas.ValidationFailure(
            "you must provide one of these fields: config, external_source_url");
      }
      if (config != null && external_source_url != null) {
        throw new Schemas.ValidationFailure(
            "you must provide ONLY ONE of these fields: config, external_source_url");
      }
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "config=" + io.akka.opal.common.util.Repr.repr(config),
          "external_source_url=" + io.akka.opal.common.util.Repr.repr(external_source_url));
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record CallbackEntry(String key, String url, HttpFetcherConfig config) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record UpdateCallback(List<Object> callbacks)
      implements io.akka.opal.common.util.Repr.Reprable {
    public UpdateCallback {
      if (callbacks == null) {
        callbacks = List.of();
      }
    }

    @Override
    public List<String> pyFields() {
      return List.of("callbacks=" + io.akka.opal.common.util.Repr.repr(callbacks));
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record DataUpdate(
      String id, List<DataSourceEntry> entries, String reason, UpdateCallback callback) {
    public DataUpdate {
      if (callback == null) {
        callback = new UpdateCallback(List.of());
      }
    }
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record DataEntryReport(
      DataSourceEntry entry, boolean fetched, boolean saved, String hash) {}

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record DataUpdateReport(
      String update_id,
      List<DataEntryReport> reports,
      String policy_hash,
      Map<String, Object> user_data) {
    public DataUpdateReport {
      if (user_data == null) {
        user_data = Map.of();
      }
    }
  }

  /** The HTTP verbs a fetcher configuration may name. */
  public enum HttpMethods {
    GET("get"),
    POST("post"),
    PUT("put"),
    PATCH("patch"),
    HEAD("head"),
    DELETE("delete");

    private final String wire;

    HttpMethods(String wire) {
      this.wire = wire;
    }

    @com.fasterxml.jackson.annotation.JsonValue
    public String wire() {
      return wire;
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public static HttpMethods from(String value) {
      for (HttpMethods candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException("invalid value: " + value);
    }
  }

  /**
   * The fetcher configuration a callback or a data entry may carry. Field order is the order
   * the source declares them in, because that order is what its printed form shows; the two
   * credential-bearing fields print redacted for the same reason they do there.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record HttpFetcherConfig(
      String fetcher,
      Map<String, String> headers,
      Boolean is_json,
      Boolean process_data,
      HttpMethods method,
      Object data)
      implements io.akka.opal.common.util.Repr.Reprable {

    public HttpFetcherConfig {
      if (is_json == null) {
        is_json = true;
      }
      if (process_data == null) {
        process_data = true;
      }
      if (method == null) {
        method = HttpMethods.GET;
      }
    }

    public static HttpFetcherConfig defaults() {
      return new HttpFetcherConfig(null, null, null, null, null, null);
    }

    /** What OPAL ships as DEFAULT_UPDATE_CALLBACK_CONFIG. */
    public static HttpFetcherConfig defaultCallbackConfig() {
      return new HttpFetcherConfig(
          null, Map.of("content-type", "application/json"), null, false, HttpMethods.POST, null);
    }

    public String methodOrGet() {
      return method == null ? "get" : method.wire();
    }

    /**
     * The exact JSON a callback's key is hashed over — SPEC-002 R97. Field order and the spaces
     * after each separator are the source's, because a callback registered by one system and
     * looked up by the other has to land on the same key.
     */
    public String pyJson() {
      java.util.LinkedHashMap<String, Object> fields = new java.util.LinkedHashMap<>();
      fields.put("fetcher", fetcher);
      fields.put("headers", headers);
      fields.put("is_json", is_json);
      fields.put("process_data", process_data);
      fields.put("method", method == null ? null : method.wire());
      fields.put("data", data);
      return io.akka.opal.common.util.PythonJson.dumps(fields);
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "fetcher=" + io.akka.opal.common.util.Repr.repr(fetcher),
          "headers='<redacted>'",
          "is_json=" + io.akka.opal.common.util.Repr.repr(is_json),
          "process_data=" + io.akka.opal.common.util.Repr.repr(process_data),
          "method=" + io.akka.opal.common.util.Repr.repr(method),
          "data='<redacted>'");
    }
  }
}
