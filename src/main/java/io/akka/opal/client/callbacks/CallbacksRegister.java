package io.akka.opal.client.callbacks;

import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.util.Hashing;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Who to tell when the store has been updated — SPEC-002 R97.
 *
 * <p>A callback's key is a digest of its URL and its configuration, so registering the same one
 * twice lands on the same key. Registering it a second time under a name of the caller's choosing
 * removes the automatically-keyed entry first: two entries would mean two calls to one endpoint
 * for one update, and the receiver has no way to tell that they are the same event.
 */
public final class CallbacksRegister {

  private static final Logger log = LoggerFactory.getLogger(CallbacksRegister.class);

  /** A URL and how to call it. */
  public record CallbackConfig(String url, Data.HttpFetcherConfig config) {}

  private final Map<String, CallbackConfig> callbacks = new LinkedHashMap<>();
  private final Data.HttpFetcherConfig defaultConfig;

  public CallbacksRegister(
      List<Object> initialCallbacks, Data.HttpFetcherConfig defaultConfig) {
    this.defaultConfig =
        defaultConfig == null ? Data.HttpFetcherConfig.defaults() : defaultConfig;
    if (initialCallbacks != null) {
      for (CallbackConfig callback : normalizeCallbacks(initialCallbacks)) {
        register(calcHash(callback.url(), callback.config()), callback.url(), callback.config());
      }
    }
    log.info("Callbacks register loaded");
  }

  /**
   * A callback is either a bare URL, which takes the default configuration, or a pair of a URL
   * and one. Anything else is logged and dropped rather than raising: these come off the wire in
   * a data update, and one malformed entry must not fail the update it rode in on.
   */
  public List<CallbackConfig> normalizeCallbacks(List<?> raw) {
    List<CallbackConfig> normalized = new ArrayList<>();
    for (Object callback : raw) {
      if (callback instanceof String url) {
        normalized.add(new CallbackConfig(url, defaultConfig));
        continue;
      }
      if (callback instanceof List<?> pair && pair.size() == 2) {
        String url = String.valueOf(pair.get(0));
        Data.HttpFetcherConfig config =
            io.akka.opal.server.pubsub.Rpc.MAPPER.convertValue(
                pair.get(1), Data.HttpFetcherConfig.class);
        normalized.add(new CallbackConfig(url, config));
        continue;
      }
      if (callback instanceof com.fasterxml.jackson.databind.JsonNode node) {
        if (node.isTextual()) {
          normalized.add(new CallbackConfig(node.asText(), defaultConfig));
          continue;
        }
        if (node.isArray() && node.size() == 2) {
          Data.HttpFetcherConfig config =
              io.akka.opal.server.pubsub.Rpc.MAPPER.convertValue(
                  node.get(1), Data.HttpFetcherConfig.class);
          normalized.add(new CallbackConfig(node.get(0).asText(), config));
          continue;
        }
      }
      log.warn(
          "Unsupported type for callback config: {}",
          callback == null ? "null" : callback.getClass().getSimpleName());
    }
    return normalized;
  }

  private void register(String key, String url, Data.HttpFetcherConfig config) {
    callbacks.put(key, new CallbackConfig(url, config));
  }

  /** R97: the digest of the URL followed by the configuration's JSON. */
  public String calcHash(String url, Data.HttpFetcherConfig config) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(url.getBytes(StandardCharsets.UTF_8));
      digest.update(config.pyJson().getBytes(StandardCharsets.UTF_8));
      return Hashing.hex(digest.digest());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public synchronized Data.CallbackEntry get(String key) {
    CallbackConfig callback = callbacks.get(key);
    if (callback == null) {
      return null;
    }
    return new Data.CallbackEntry(key, callback.url(), callback.config());
  }

  public synchronized String put(String url, Data.HttpFetcherConfig config, String key) {
    Data.HttpFetcherConfig callbackConfig = config == null ? defaultConfig : config;
    String autoKey = calcHash(url, callbackConfig);
    String callbackKey = key == null ? autoKey : key;
    remove(autoKey);
    register(callbackKey, url, callbackConfig);
    return callbackKey;
  }

  public synchronized void remove(String key) {
    callbacks.remove(key);
  }

  public synchronized List<Data.CallbackEntry> all() {
    List<Data.CallbackEntry> entries = new ArrayList<>();
    callbacks.forEach(
        (key, callback) ->
            entries.add(new Data.CallbackEntry(key, callback.url(), callback.config())));
    return entries;
  }
}
