package io.akka.opal.server.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.auth.Unauthorized;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R58 to R64, driven frame by frame.
 *
 * <p>Frames rather than method calls: the protocol is what a real OPAL client speaks, and the
 * shape of the answer — the {@code result_type} beside the result, the {@code call_id} echoed
 * back — is as much a part of it as the value.
 */
class PubSubProtocolTest {

  private record Channel(RpcChannel channel, List<String> sent) {}

  private static Channel channel(EventNotifier notifier) {
    List<String> sent = new ArrayList<>();
    return new Channel(new RpcChannel(notifier, sent::add), sent);
  }

  /** R59 and R61: subscribe answers a boolean, and echoes the call id. */
  @Test
  void subscribeAnswersTrueAndEchoesTheCallId() throws Exception {
    EventNotifier notifier = new EventNotifier();
    Channel peer = channel(notifier);
    peer.channel()
        .onMessage(
            Rpc.serialize(
                Rpc.RpcMessage.request("subscribe", Map.of("topics", List.of("policy_data")), "c1")));
    assertEquals(1, peer.sent().size());
    JsonNode answer = Rpc.MAPPER.readTree(peer.sent().get(0));
    assertTrue(answer.path("response").path("result").asBoolean());
    assertEquals("bool", answer.path("response").path("result_type").asText());
    assertEquals("c1", answer.path("response").path("call_id").asText());
  }

  /** R59: a method whose name begins with an underscore is refused, bar the two built-ins. */
  @Test
  void protectedMethodsAreIgnored() {
    EventNotifier notifier = new EventNotifier();
    Channel peer = channel(notifier);
    peer.channel().onMessage(Rpc.serialize(Rpc.RpcMessage.request("_secret_", Map.of(), "c1")));
    assertEquals(0, peer.sent().size());

    peer.channel().onMessage(Rpc.serialize(Rpc.RpcMessage.request("_ping_", Map.of(), "c2")));
    assertEquals(1, peer.sent().size());
    assertTrue(peer.sent().get(0).contains("pong"));

    peer.channel()
        .onMessage(Rpc.serialize(Rpc.RpcMessage.request("_get_channel_id_", Map.of(), "c3")));
    assertEquals(2, peer.sent().size());
    assertTrue(peer.sent().get(1).contains(peer.channel().id()));
  }

  /** R60: one publication matching three of a subscriber's topics produces three notifications. */
  @Test
  void everyMatchedTopicProducesItsOwnNotification() throws Exception {
    EventNotifier notifier = new EventNotifier();
    Channel subscriber = channel(notifier);
    subscriber
        .channel()
        .onMessage(
            Rpc.serialize(
                Rpc.RpcMessage.request(
                    "subscribe",
                    Map.of(
                        "topics",
                        List.of("policy_data", "policy_data/users", "policy_data/users/keys")),
                    "c1")));
    subscriber.sent().clear();

    notifier.notify(
        List.of("policy_data", "policy_data/users", "policy_data/users/keys"),
        Map.of("reason", "probe"),
        null,
        null);

    assertEquals(3, subscriber.sent().size());
    List<String> topics = new ArrayList<>();
    for (String frame : subscriber.sent()) {
      JsonNode message = Rpc.MAPPER.readTree(frame);
      assertEquals("notify", message.path("request").path("method").asText());
      topics.add(
          message.path("request").path("arguments").path("subscription").path("topic").asText());
    }
    assertEquals(
        List.of("policy_data", "policy_data/users", "policy_data/users/keys"), topics);
  }

  /** R62: a subscriber of the sentinel receives every publication, named by the real topic. */
  @Test
  void theSentinelTopicReceivesEverything() throws Exception {
    EventNotifier notifier = new EventNotifier();
    List<String> received = new ArrayList<>();
    notifier.subscribe(
        "internal", List.of(Rpc.ALL_TOPICS), (subscription, data) -> received.add(subscription.topic()),
        null);

    notifier.notify(List.of("anything"), null, null, null);
    assertEquals(List.of("anything"), received);
  }

  /** R60: the notifier's own id is not notified of its own publication. */
  @Test
  void aPublisherIsNotNotifiedOfItsOwnPublication() {
    EventNotifier notifier = new EventNotifier();
    List<String> received = new ArrayList<>();
    notifier.subscribe("me", List.of("t"), (subscription, data) -> received.add("got"), null);
    notifier.notify(List.of("t"), null, "me", null);
    assertEquals(List.of(), received);
    notifier.notify(List.of("t"), null, "someone-else", null);
    assertEquals(List.of("got"), received);
  }

  /** R61 and R63: a refusal by a restriction is a false answer, not an error frame. */
  @Test
  void aRefusedSubscriptionAnswersFalse() throws Exception {
    EventNotifier notifier = new EventNotifier();
    notifier.addChannelRestriction(
        (topics, ignored) -> {
          throw new Unauthorized("Invalid 'topics' to subscribe " + topics);
        });
    Channel peer = channel(notifier);
    peer.channel()
        .onMessage(
            Rpc.serialize(Rpc.RpcMessage.request("subscribe", Map.of("topics", List.of("x")), "c1")));
    JsonNode answer = Rpc.MAPPER.readTree(peer.sent().get(0));
    assertFalse(answer.path("response").path("result").asBoolean());
  }

  /** R64: the query parameter wins, then host and port, then a generated id. */
  @Test
  void clientIdsMatchTheSource() {
    assertEquals(
        "probe-1",
        ClientTracker.clientIdFor("127.0.0.1", 4000, Map.of("__opal_client_id", "probe-1")));
    assertEquals("host:127.0.0.1:4000", ClientTracker.clientIdFor("127.0.0.1", 4000, Map.of()));
    assertTrue(ClientTracker.clientIdFor(null, null, Map.of()).startsWith("opal:"));
  }

  /** R64: two connections sharing a client id share one record and a reference count. */
  @Test
  void twoConnectionsSharingAnIdShareOneRecord() {
    ClientTracker tracker = new ClientTracker();
    Map<String, String> params = Map.of("__opal_client_id", "probe-1");
    tracker.newClient("127.0.0.1", 1, params);
    tracker.newClient("127.0.0.1", 2, params);
    assertEquals(1, tracker.clients().size());
    assertEquals(2, tracker.clients().get("probe-1").refcount);

    tracker.releaseClient("probe-1");
    assertEquals(1, tracker.clients().size());
    tracker.releaseClient("probe-1");
    assertEquals(0, tracker.clients().size());
  }

  /** R66: what the backbone carries is the publication, and a replica skips its own. */
  @Test
  void aMemoryBackboneRelaysWhatItIsGiven() {
    Broadcaster broadcaster = Broadcasters.forUri("memory://", "EventNotifier");
    List<Broadcaster.BroadcastNotification> read = new ArrayList<>();
    broadcaster.start(read::add);
    broadcaster.publish(
        new Broadcaster.BroadcastNotification(broadcaster.id(), List.of("t"), Map.of("a", 1)));
    assertEquals(1, read.size());
    assertEquals(List.of("t"), read.get(0).topics());
    broadcaster.close();
  }
}
