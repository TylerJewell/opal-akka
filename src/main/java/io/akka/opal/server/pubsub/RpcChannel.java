package io.akka.opal.server.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.auth.Unauthorized;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One websocket connection, and the six methods a peer may call on it — SPEC-002 R59 and R61.
 *
 * <p>A method name beginning with an underscore is refused unless it is one of the two built-ins,
 * which is what keeps the dispatch from reaching anything the protocol did not mean to expose.
 * {@code subscribe} answers false on any failure, including a refusal, rather than raising: the
 * peer's own library treats an error frame as a protocol fault and drops the connection, and a
 * refused subscription is not a protocol fault.
 */
public final class RpcChannel {

  private static final Logger log = LoggerFactory.getLogger(RpcChannel.class);

  private final String id = EventNotifier.generateId();
  private final EventNotifier notifier;
  private final Consumer<String> send;
  private final Map<String, Object> context = new LinkedHashMap<>();

  private volatile String otherChannelId;

  public RpcChannel(EventNotifier notifier, Consumer<String> send) {
    this.notifier = notifier;
    this.send = send;
  }

  public String id() {
    return id;
  }

  public Map<String, Object> context() {
    return context;
  }

  public String otherChannelId() {
    return otherChannelId;
  }

  public void setOtherChannelId(String value) {
    this.otherChannelId = value;
  }

  /** The subscriber id this channel's subscriptions are recorded under. */
  public String subscriberId(boolean useRemoteId) {
    if (useRemoteId && otherChannelId != null) {
      return otherChannelId;
    }
    return id;
  }

  public void onMessage(String frame) {
    Rpc.RpcMessage message;
    try {
      message = Rpc.parse(frame);
    } catch (Exception e) {
      log.error("Failed to parse message: {}", frame, e);
      return;
    }
    if (message.request() != null) {
      onRequest(message.request());
    }
    if (message.response() != null) {
      onResponse(message.response());
    }
  }

  private void onRequest(Rpc.RpcRequest request) {
    String method = request.method();
    if (method == null) {
      return;
    }
    if (method.startsWith("_") && !Rpc.EXPOSED_BUILT_IN_METHODS.contains(method)) {
      log.debug("ignoring protected method {}", method);
      return;
    }
    Object result;
    String resultType;
    switch (method) {
      case "subscribe" -> {
        result = subscribe(stringList(request.arguments().get("topics")));
        resultType = "bool";
      }
      case "unsubscribe" -> {
        result = unsubscribe(stringList(request.arguments().get("topics")));
        resultType = "bool";
      }
      case "publish" -> {
        Map<String, Object> arguments = request.arguments();
        result =
            publish(
                stringList(arguments.get("topics")),
                arguments.get("data"),
                arguments.get("notifier_id") == null
                    ? null
                    : String.valueOf(arguments.get("notifier_id")));
        resultType = "bool";
      }
      case "ping" -> {
        result = Rpc.PING_RESPONSE;
        resultType = "str";
      }
      case "_ping_" -> {
        result = Rpc.PING_RESPONSE;
        resultType = "str";
      }
      case "_get_channel_id_" -> {
        result = id;
        resultType = "str";
      }
      default -> {
        log.debug("no such method {}", method);
        return;
      }
    }
    send.accept(Rpc.serialize(Rpc.RpcMessage.response(result, resultType, request.call_id())));
  }

  private void onResponse(Rpc.RpcResponse response) {
    // The only call this side makes to a peer is notify(), whose answer nothing waits on;
    // a channel-id sync arrives here when the peer answers _get_channel_id_.
    if (response.result() != null && response.call_id() != null
        && response.call_id().startsWith("channel-id:")) {
      setOtherChannelId(String.valueOf(response.result()));
    }
  }

  /** R61: true on success, false on any failure at all. */
  public boolean subscribe(List<String> topics) {
    try {
      notifier.subscribe(
          subscriberId(false),
          topics,
          (subscription, data) -> notifyPeer(subscription, data),
          this);
      return true;
    } catch (Unauthorized | EventNotifier.Refused e) {
      log.info("Refused subscription: {}", e.getMessage());
      return false;
    } catch (Exception e) {
      log.warn("Failed to subscribe to RPC events notifier {}", topics, e);
      return false;
    }
  }

  public boolean unsubscribe(List<String> topics) {
    List<String> present = new ArrayList<>();
    for (String topic : topics) {
      if (notifier.hasTopic(topic)) {
        present.add(topic);
      } else {
        log.warn("Cannot unsubscribe topic '{}' which is not subscribed.", topic);
      }
    }
    notifier.unsubscribe(subscriberId(false), present);
    return true;
  }

  public boolean publish(List<String> topics, Object data, String notifierId) {
    try {
      notifier.notify(topics, data, notifierId == null ? id : notifierId, this);
      return true;
    } catch (Exception e) {
      log.error("Failed to publish to events notifier {}", topics, e);
      return false;
    }
  }

  /** R60: the peer is called back with the matched topic and the published data. */
  public void notifyPeer(EventNotifier.Subscription subscription, Object data) {
    Map<String, Object> arguments = new LinkedHashMap<>();
    arguments.put("subscription", Rpc.tree(subscription));
    arguments.put("data", data);
    send.accept(
        Rpc.serialize(
            Rpc.RpcMessage.request("notify", arguments, EventNotifier.generateId())));
  }

  /** Asks the peer for its own channel id, which statistics uses as the subscriber id. */
  public void requestOtherChannelId() {
    send.accept(
        Rpc.serialize(
            Rpc.RpcMessage.request(
                "_get_channel_id_", Map.of(), "channel-id:" + EventNotifier.generateId())));
  }

  @SuppressWarnings("unchecked")
  static List<String> stringList(Object value) {
    List<String> out = new ArrayList<>();
    if (value instanceof List<?> list) {
      for (Object item : list) {
        out.add(String.valueOf(item));
      }
    } else if (value instanceof JsonNode node && node.isArray()) {
      node.forEach(item -> out.add(item.asText()));
    } else if (value != null) {
      out.add(String.valueOf(value));
    }
    return out;
  }
}
