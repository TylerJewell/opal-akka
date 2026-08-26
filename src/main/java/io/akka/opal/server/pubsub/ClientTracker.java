package io.akka.opal.server.pubsub;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Who is connected — SPEC-002 R64 and R65.
 *
 * <p>Two connections that name the same client id share one record and a reference count, so a
 * client that reconnects before its old socket has closed appears once rather than twice, and
 * the record survives until the last of them goes.
 */
public final class ClientTracker {

  public static final String CLIENT_INFO_PARAM_PREFIX = "__opal_";
  public static final String CLIENT_INFO_CLIENT_ID = CLIENT_INFO_PARAM_PREFIX + "client_id";

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public static final class ClientInfo {
    public String client_id;
    public String source_host;
    public Integer source_port;
    public double connect_time;
    public Set<String> subscribed_topics = new LinkedHashSet<>();
    public int refcount;
    public Map<String, String> query_params = new LinkedHashMap<>();

    public String getClient_id() {
      return client_id;
    }

    public String getSource_host() {
      return source_host;
    }

    public Integer getSource_port() {
      return source_port;
    }

    public double getConnect_time() {
      return connect_time;
    }

    public Set<String> getSubscribed_topics() {
      return subscribed_topics;
    }

    public int getRefcount() {
      return refcount;
    }

    public Map<String, String> getQuery_params() {
      return query_params;
    }
  }

  private final Map<String, ClientInfo> clientsByIds = new LinkedHashMap<>();

  public synchronized Map<String, ClientInfo> clients() {
    return new LinkedHashMap<>(clientsByIds);
  }

  /** R64: the query parameter wins, then host and port, then a generated id. */
  public static String clientIdFor(
      String sourceHost, Integer sourcePort, Map<String, String> queryParams) {
    if (queryParams != null && queryParams.containsKey(CLIENT_INFO_CLIENT_ID)) {
      return queryParams.get(CLIENT_INFO_CLIENT_ID);
    }
    if (sourceHost != null && sourcePort != null) {
      return "host:" + sourceHost + ":" + sourcePort;
    }
    return "opal:" + UUID.randomUUID().toString().replace("-", "");
  }

  public synchronized ClientInfo newClient(
      String sourceHost, Integer sourcePort, Map<String, String> queryParams) {
    String clientId = clientIdFor(sourceHost, sourcePort, queryParams);
    ClientInfo info = clientsByIds.remove(clientId);
    if (info == null) {
      info = new ClientInfo();
      info.client_id = clientId;
      info.source_host = sourceHost;
      info.source_port = sourcePort;
      info.connect_time = System.currentTimeMillis() / 1000.0;
      info.query_params = queryParams == null ? new LinkedHashMap<>() : queryParams;
    }
    info.refcount++;
    clientsByIds.put(clientId, info);
    return info;
  }

  public synchronized void releaseClient(String clientId) {
    ClientInfo info = clientsByIds.remove(clientId);
    if (info == null) {
      return;
    }
    info.refcount--;
    if (info.refcount >= 1) {
      clientsByIds.put(clientId, info);
    }
  }

  public synchronized void onSubscribe(ClientInfo info, List<String> topics) {
    if (info != null) {
      info.subscribed_topics.addAll(topics);
    }
  }

  public synchronized void onUnsubscribe(ClientInfo info, List<String> topics) {
    if (info != null) {
      topics.forEach(info.subscribed_topics::remove);
    }
  }

  public synchronized List<String> clientIds() {
    return new ArrayList<>(clientsByIds.keySet());
  }
}
