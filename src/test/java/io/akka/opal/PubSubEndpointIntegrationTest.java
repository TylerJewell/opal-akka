package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.javasdk.testkit.WebSocketRouteTester;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The pub/sub channel over a real websocket — SPEC-002 R58 to R65, and R1.3 of RENDERING.md.
 *
 * <p>A subscribe, a publish and the notification that follows, in that order and over the wire.
 * That sequence is the whole product: everything else in OPAL exists to decide what to publish.
 */
public class PubSubEndpointIntegrationTest extends TestKitSupport {

  private static final scala.concurrent.duration.FiniteDuration WAIT =
      scala.concurrent.duration.FiniteDuration.apply(
          10, java.util.concurrent.TimeUnit.SECONDS);

  private WebSocketRouteTester.WsConnection<String> connect() {
    return testKit.getSelfWebSocketRouteTester().wsTextConnection("/ws");
  }

  private static JsonNode read(String frame) {
    try {
      return Rpc.MAPPER.readTree(frame);
    } catch (Exception e) {
      throw new IllegalStateException("not a frame: " + frame, e);
    }
  }

  /** R59, R61: a subscribe is answered with a boolean carrying the call id back. */
  @Test
  public void subscribingOverTheWireIsAnswered() {
    var connection = connect();
    connection.publisher().sendNext(
        Rpc.serialize(
            Rpc.RpcMessage.request("subscribe", Map.of("topics", List.of("policy_data")), "c1")));
    connection.subscriber().request(1);
    JsonNode answer = read(connection.subscriber().expectNext(WAIT));
    assertTrue(answer.path("response").path("result").asBoolean());
    assertEquals("c1", answer.path("response").path("call_id").asText());
    connection.publisher().sendComplete();
  }

  /** R60: a publication reaches a subscriber as a {@code notify} naming the matched topic. */
  @Test
  public void aPublicationReachesASubscriber() {
    var subscriber = connect();
    subscriber.publisher().sendNext(
        Rpc.serialize(
            Rpc.RpcMessage.request("subscribe", Map.of("topics", List.of("policy_data")), "s1")));
    subscriber.subscriber().request(1);
    read(subscriber.subscriber().expectNext(WAIT));

    var publisher = connect();
    publisher.publisher().sendNext(
        Rpc.serialize(
            Rpc.RpcMessage.request(
                "publish",
                Map.of(
                    "topics", List.of("policy_data"),
                    "data", Map.of("reason", "probe"),
                    "sync", true),
                "p1")));
    publisher.subscriber().request(1);
    JsonNode published = read(publisher.subscriber().expectNext(WAIT));
    assertTrue(published.path("response").path("result").asBoolean());

    subscriber.subscriber().request(1);
    JsonNode notification = read(subscriber.subscriber().expectNext(WAIT));
    assertEquals("notify", notification.path("request").path("method").asText());
    assertEquals(
        "policy_data",
        notification.path("request").path("arguments").path("subscription").path("topic").asText());
    assertEquals(
        "probe",
        notification.path("request").path("arguments").path("data").path("reason").asText());

    subscriber.publisher().sendComplete();
    publisher.publisher().sendComplete();
  }

  /** R59: the two built-in underscore methods answer; anything else beginning with one does not. */
  @Test
  public void theBuiltInMethodsAnswer() {
    var connection = connect();
    connection.publisher().sendNext(
        Rpc.serialize(Rpc.RpcMessage.request("_ping_", Map.of(), "c1")));
    connection.subscriber().request(1);
    assertEquals("pong", read(connection.subscriber().expectNext(WAIT))
        .path("response").path("result").asText());

    connection.publisher().sendNext(
        Rpc.serialize(Rpc.RpcMessage.request("_get_channel_id_", Map.of(), "c2")));
    connection.subscriber().request(1);
    assertTrue(
        read(connection.subscriber().expectNext(WAIT))
            .path("response").path("result").asText().length() > 0);
    connection.publisher().sendComplete();
  }

  /** R62: the purge channel is refused to an external peer, and so is the sentinel. */
  @Test
  public void theInternalChannelsAreRefusedToAPeer() {
    var connection = connect();
    connection.publisher().sendNext(
        Rpc.serialize(
            Rpc.RpcMessage.request(
                "subscribe", Map.of("topics", List.of("__opal_scope_purge__")), "c1")));
    connection.subscriber().request(1);
    assertTrue(!read(connection.subscriber().expectNext(WAIT))
        .path("response").path("result").asBoolean());

    connection.publisher().sendNext(
        Rpc.serialize(
            Rpc.RpcMessage.request(
                "subscribe", Map.of("topics", List.of(Rpc.ALL_TOPICS)), "c2")));
    connection.subscriber().request(1);
    assertTrue(!read(connection.subscriber().expectNext(WAIT))
        .path("response").path("result").asBoolean());
    connection.publisher().sendComplete();
  }

  /**
   * R64 and R65: a connection naming a client id appears under that id, and goes when it closes.
   *
   * <p>This is the half that gets skipped. A register that only ever grows looks correct for as
   * long as nothing disconnects, and a fleet that reconnects on every deploy is exactly where it
   * stops being.
   */
  @Test
  public void theClientRegisterGainsAndLosesAConnection() throws Exception {
    var connection =
        testKit.getSelfWebSocketRouteTester().wsTextConnection("/ws?__opal_client_id=probe-1");
    connection.publisher().sendNext(
        Rpc.serialize(
            Rpc.RpcMessage.request("subscribe", Map.of("topics", List.of("policy_data")), "c1")));
    connection.subscriber().request(1);
    read(connection.subscriber().expectNext(WAIT));

    JsonNode connected = clientInfo();
    assertTrue(connected.has("probe-1"), "the connection is registered: " + connected);
    assertEquals(1, connected.get("probe-1").get("refcount").asInt());
    assertEquals(
        List.of("policy_data"),
        SourceAnswers.strings(connected.get("probe-1").get("subscribed_topics")));

    connection.publisher().sendComplete();

    JsonNode after = clientInfo();
    for (int attempt = 0; attempt < 50 && after.has("probe-1"); attempt++) {
      Thread.sleep(100);
      after = clientInfo();
    }
    assertTrue(!after.has("probe-1"), "the connection is gone once it closes: " + after);
  }

  private JsonNode clientInfo() {
    StrictResponse<ByteString> response = httpClient.GET("/pubsub_client_info").invoke();
    return read(response.body().decodeString(StandardCharsets.UTF_8));
  }
}
