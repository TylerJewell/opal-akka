package io.akka.opal.server.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R66 and R67 — the backbone between replicas, and the traffic that keeps it honest.
 *
 * <p>The transport is chosen by the scheme of one setting, and choosing it wrongly is silent: a
 * deployment that meant Redis and got nothing has a fleet that agrees with itself on each replica
 * and not across them, which looks exactly like a working one until two replicas are asked the
 * same question.
 */
class BroadcastTest {

  /** R66: which transport each scheme selects, and what an absent setting means. */
  @Test
  void theSchemeSelectsTheTransport() {
    assertNull(Broadcasters.forUri(null, "channel"), "no backbone configured");

    assertNotNull(Broadcasters.forUri("redis://127.0.0.1:6379", "channel"));
    assertNotNull(Broadcasters.forUri("kafka://127.0.0.1:9092", "channel"));
    assertNotNull(Broadcasters.forUri("postgres://user:pw@127.0.0.1:5432/opal", "channel"));
    assertNotNull(Broadcasters.forUri("memory://", "channel"));

    assertEquals(
        Broadcasters.forUri("redis://127.0.0.1:6379", "channel").getClass(),
        Broadcasters.forUri("rediss://127.0.0.1:6379", "channel").getClass(),
        "the TLS spelling of a scheme is the same transport");
    assertEquals(
        Broadcasters.forUri("postgres://h/db", "channel").getClass(),
        Broadcasters.forUri("postgresql://h/db", "channel").getClass());

    // A value that is set and unusable is refused rather than read as unset: a deployment that
    // meant to configure a backbone and mistyped it would otherwise run as a split fleet.
    assertThrows(IllegalArgumentException.class, () -> Broadcasters.forUri("", "channel"));
    assertThrows(
        IllegalArgumentException.class, () -> Broadcasters.forUri("amqp://h", "channel"));
  }

  /** The in-process transport, which relays to this replica's own reader and nowhere else. */
  @Test
  void theInProcessTransportRelaysToItsOwnReader() {
    Broadcaster broadcaster = Broadcasters.forUri("memory://", "channel");
    List<List<String>> seen = new ArrayList<>();
    broadcaster.start(notification -> seen.add(notification.topics()));
    broadcaster.publish(
        new Broadcaster.BroadcastNotification(broadcaster.id(), List.of("a", "b"), null));
    assertEquals(List.of(List.of("a", "b")), seen);
  }

  /** R66: a replica carries an identity, so it can tell its own publications from a peer's. */
  @Test
  void eachReplicaCarriesItsOwnIdentity() {
    Broadcaster first = Broadcasters.forUri("redis://127.0.0.1:6379", "channel");
    Broadcaster second = Broadcasters.forUri("redis://127.0.0.1:6379", "channel");
    assertNotNull(first.id());
    assertTrue(!first.id().equals(second.id()), "two replicas are not one");
  }

  /** R67: with a backbone and a positive interval, the topic is published on the interval. */
  @Test
  void theKeepalivePublishesOnItsOwnTopic() throws Exception {
    List<String> published = new ArrayList<>();
    CountDownLatch fired = new CountDownLatch(1);
    try (BroadcastKeepalive keepalive =
        BroadcastKeepalive.start(
            true,
            1,
            "__broadcast_session_keepalive__",
            topic -> {
              published.add(topic);
              fired.countDown();
            })) {
      assertNotNull(keepalive);
      assertTrue(fired.await(10, TimeUnit.SECONDS), "it fired");
      assertEquals("__broadcast_session_keepalive__", published.get(0));
    }
  }

  /** R67: the two ways it is off — no backbone, and a non-positive interval. */
  @Test
  void theKeepaliveIsOffWithoutABackboneOrAnInterval() {
    assertNull(
        BroadcastKeepalive.start(false, 3600, "t", topic -> {}), "nothing to keep alive");
    assertNull(BroadcastKeepalive.start(true, 0, "t", topic -> {}), "turned off by its interval");
    assertNull(BroadcastKeepalive.start(true, -1, "t", topic -> {}), "and by a negative one");
  }

  /** A failing publish does not stop the timer, or one dropped connection ends the keepalive. */
  @Test
  void aFailedPublishDoesNotStopTheTimer() throws Exception {
    CountDownLatch twice = new CountDownLatch(2);
    try (BroadcastKeepalive keepalive =
        BroadcastKeepalive.start(
            true,
            1,
            "t",
            topic -> {
              twice.countDown();
              throw new IllegalStateException("backbone is down");
            })) {
      assertInstanceOf(BroadcastKeepalive.class, keepalive);
      assertTrue(twice.await(15, TimeUnit.SECONDS), "it kept going after the first failure");
    }
  }
}
