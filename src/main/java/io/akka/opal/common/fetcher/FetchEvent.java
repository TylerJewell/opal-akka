package io.akka.opal.common.fetcher;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/**
 * One queued fetch — SPEC-002 R145.
 *
 * <p>An event names the provider to use as a string rather than holding one, so a fetch can be
 * described somewhere that has never loaded the class that performs it. The id is filled in by
 * the engine when the event is queued, which is what makes a fetch traceable from the queue to
 * the failure handler.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class FetchEvent {

  public static final String DEFAULT_FETCHER = "HttpFetchProvider";

  private String id;
  private String name;
  private String fetcher;
  private String url;
  private Map<String, Object> config;
  private Map<String, Object> retry;

  public FetchEvent() {}

  public FetchEvent(String url, String fetcher, Map<String, Object> config,
      Map<String, Object> retry) {
    this.url = url;
    this.fetcher = fetcher;
    this.config = config;
    this.retry = retry;
  }

  public String id() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String name() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String fetcher() {
    return fetcher;
  }

  public void setFetcher(String fetcher) {
    this.fetcher = fetcher;
  }

  public String url() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public Map<String, Object> config() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public Map<String, Object> retry() {
    return retry;
  }

  public void setRetry(Map<String, Object> retry) {
    this.retry = retry;
  }

  /** The url with any credentials removed, which is the only form fit for a log line. */
  @Override
  public String toString() {
    return "FetchEvent(id="
        + id
        + ", fetcher="
        + fetcher
        + ", url="
        + io.akka.opal.common.util.Urls.redactUrl(url)
        + ", config='<redacted>')";
  }
}
