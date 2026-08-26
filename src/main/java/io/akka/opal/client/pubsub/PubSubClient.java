package io.akka.opal.client.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.pubsub.EventNotifier;
import io.akka.opal.server.pubsub.Rpc;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client half of OPAL's pub/sub channel — SPEC-002 R92.
 *
 * <p>A reconnect is not a resumption. Nothing is replayed, so on every connection — the first and
 * every one after it — the client re-triggers a full policy update and re-fetches the whole base
 * data configuration. That is the only recovery the protocol offers, and it is why the connect
 * callback does the same work whether it is the first connection or the fiftieth.
 */
public final class PubSubClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PubSubClient.class);

  private final URI serverUri;
  private final List<String> topics;
  private final String token;
  private final BiConsumer<String, JsonNode> onNotification;
  private final Consumer<PubSubClient> onConnect;
  private final Runnable onDisconnect;
  private final Duration keepAlive;

  /** R251: which shard this client belongs to, announced on every connection. */
  private volatile String shardId;

  public PubSubClient withShardId(String value) {
    this.shardId = value;
    return this;
  }

  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicBoolean connected = new AtomicBoolean();
  private volatile WebSocket socket;
  private volatile String channelId;
  private volatile CountDownLatch ready = new CountDownLatch(1);
  private ScheduledExecutorService keepAliveScheduler;
  private Thread reconnectThread;

  public PubSubClient(
      String serverUri,
      List<String> topics,
      String token,
      BiConsumer<String, JsonNode> onNotification,
      Consumer<PubSubClient> onConnect,
      Runnable onDisconnect,
      int keepAliveSeconds) {
    this.serverUri = URI.create(serverUri);
    this.topics = topics;
    this.token = token;
    this.onNotification = onNotification;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
    this.keepAlive = Duration.ofSeconds(Math.max(0, keepAliveSeconds));
  }

  public boolean connected() {
    return connected.get();
  }

  public String channelId() {
    return channelId;
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    reconnectThread =
        new Thread(
            () -> {
              while (running.get()) {
                try {
                  connectOnce();
                } catch (Exception e) {
                  log.info("pub/sub connection failed: {}", e.toString());
                }
                if (!running.get()) {
                  return;
                }
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            },
            "opal-pubsub-client");
    reconnectThread.setDaemon(true);
    reconnectThread.start();
  }

  private void connectOnce() throws Exception {
    CountDownLatch closed = new CountDownLatch(1);
    ready = new CountDownLatch(1);
    HttpClient http = Http.forClient();
    WebSocket.Builder builder = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(10));
    if (token != null) {
      builder.header("Authorization", "Bearer " + token);
    }
    // R251: which shard this client belongs to, which a sharded deployment routes on.
    if (shardId != null && !shardId.isEmpty()) {
      builder.header("X-Shard-ID", shardId);
    }
    StringBuilder partial = new StringBuilder();
    socket =
        builder
            .buildAsync(
                serverUri,
                new WebSocket.Listener() {
                  @Override
                  public void onOpen(WebSocket webSocket) {
                    webSocket.request(1);
                  }

                  @Override
                  public CompletionStage<?> onText(
                      WebSocket webSocket, CharSequence data, boolean last) {
                    partial.append(data);
                    if (last) {
                      String frame = partial.toString();
                      partial.setLength(0);
                      handleFrame(webSocket, frame);
                    }
                    webSocket.request(1);
                    return null;
                  }

                  @Override
                  public CompletionStage<?> onClose(
                      WebSocket webSocket, int statusCode, String reason) {
                    connected.set(false);
                    closed.countDown();
                    return null;
                  }

                  @Override
                  public void onError(WebSocket webSocket, Throwable error) {
                    connected.set(false);
                    closed.countDown();
                  }
                })
            .join();

    connected.set(true);
    log.info("Connected to server");
    subscribe(topics);
    ready.countDown();
    if (onConnect != null) {
      onConnect.accept(this);
    }
    startKeepAlive();

    closed.await();
    stopKeepAlive();
    log.info("Disconnected from server");
    if (onDisconnect != null) {
      onDisconnect.run();
    }
  }

  private void handleFrame(WebSocket webSocket, String frame) {
    try {
      Rpc.RpcMessage message = Rpc.parse(frame);
      if (message.request() != null) {
        Rpc.RpcRequest request = message.request();
        if ("notify".equals(request.method())) {
          JsonNode subscription = Rpc.MAPPER.valueToTree(request.arguments().get("subscription"));
          JsonNode data = Rpc.MAPPER.valueToTree(request.arguments().get("data"));
          // R252: a multi-tenant deployment prefixes a topic with its own application id and two
          // colons. The prefix is not part of the topic this client subscribed to, so it is taken
          // off before the topic is matched — otherwise every notification arrives unrecognised.
          String topic = stripTenantPrefix(subscription.path("topic").asText());
          onNotification.accept(topic, data);
          send(webSocket, Rpc.RpcMessage.response(null, "NoneType", request.call_id()));
          return;
        }
        if ("_get_channel_id_".equals(request.method())) {
          if (channelId == null) {
            channelId = EventNotifier.generateId();
          }
          send(webSocket, Rpc.RpcMessage.response(channelId, "str", request.call_id()));
          return;
        }
        if ("_ping_".equals(request.method()) || "ping".equals(request.method())) {
          send(webSocket, Rpc.RpcMessage.response(Rpc.PING_RESPONSE, "str", request.call_id()));
        }
      }
    } catch (Exception e) {
      log.debug("could not read a pub/sub frame: {}", e.toString());
    }
  }

  /** R252: what a topic is called once the tenant prefix is off it. */
  public static String stripTenantPrefix(String topic) {
    if (topic == null) {
      return null;
    }
    int separator = topic.indexOf("::");
    return separator < 0 ? topic : topic.substring(separator + 2);
  }

  private void send(WebSocket webSocket, Rpc.RpcMessage message) {
    webSocket.sendText(Rpc.serialize(message), true);
  }

  public void subscribe(List<String> topicList) {
    Map<String, Object> arguments = new LinkedHashMap<>();
    arguments.put("topics", topicList);
    call("subscribe", arguments);
  }

  public void publish(List<String> topicList, Object data) {
    Map<String, Object> arguments = new LinkedHashMap<>();
    arguments.put("topics", topicList);
    arguments.put("data", data);
    arguments.put("sync", true);
    call("publish", arguments);
  }

  private void call(String method, Map<String, Object> arguments) {
    WebSocket current = socket;
    if (current == null) {
      return;
    }
    current.sendText(
        Rpc.serialize(Rpc.RpcMessage.request(method, arguments, EventNotifier.generateId())),
        true);
  }

  /** Blocks until the subscription has been sent, which is what a publisher waits on. */
  public boolean waitUntilReady(Duration timeout) {
    try {
      return ready.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void startKeepAlive() {
    if (keepAlive.isZero()) {
      return;
    }
    keepAliveScheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-pubsub-keepalive");
              thread.setDaemon(true);
              return thread;
            });
    keepAliveScheduler.scheduleWithFixedDelay(
        () -> call("_ping_", Map.of()),
        keepAlive.toMillis(),
        keepAlive.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void stopKeepAlive() {
    if (keepAliveScheduler != null) {
      keepAliveScheduler.shutdownNow();
      keepAliveScheduler = null;
    }
  }

  @Override
  public void close() {
    running.set(false);
    stopKeepAlive();
    WebSocket current = socket;
    if (current != null) {
      current.sendClose(WebSocket.NORMAL_CLOSURE, "bye");
    }
    if (reconnectThread != null) {
      reconnectThread.interrupt();
      reconnectThread = null;
    }
  }

  /** The topics this client asked for, for the statistics message it publishes on connect. */
  public List<String> topics() {
    return new ArrayList<>(topics);
  }
}
