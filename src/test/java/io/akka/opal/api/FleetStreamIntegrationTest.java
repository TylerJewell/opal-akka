package io.akka.opal.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.http.javadsl.model.sse.ServerSentEvent;
import akka.javasdk.testkit.TestKitSupport;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rule 20, and RENDERING.md R1.3 — the half that gets skipped, because a stream that
 * never drops looks identical to a correct one until it drops.
 *
 * <p>The stream is hung up on part-way through and reconnected with the position last seen, and
 * what arrives on the second connection is checked for both halves: nothing missing and nothing
 * repeated.
 */
public class FleetStreamIntegrationTest extends TestKitSupport {

  private static final Duration WAIT = Duration.ofSeconds(20);

  private PropagationEndpoint.Accepted publish(String address, String destination, String value) {
    return httpClient
        .POST("/changes")
        .withRequestBody(
            new PropagationEndpoint.PublishRequest(
                null,
                "stream",
                List.of(new PropagationEndpoint.EntryRequest(address, destination, value))))
        .responseBodyAs(PropagationEndpoint.Accepted.class)
        .invoke()
        .body();
  }

  private static List<Long> positionsOf(List<ServerSentEvent> events) {
    return events.stream()
        .map(e -> Long.parseLong(e.getId().orElseThrow()))
        .toList();
  }

  @Test
  public void aReaderThatLosesTheStreamResumesWithNothingMissingAndNothingRepeated() {
    var path = "/channels/policy_data/stream";
    IntStream.rangeClosed(1, 5).forEach(n -> publish("policy_data/stream", "/s", "v" + n));

    // First connection: read five, then hang up.
    var first = positionsOf(testKit.getSelfSseRouteTester().receiveFirstN(path, 5, WAIT));
    assertThat(first).isEqualTo(List.of(1L, 2L, 3L, 4L, 5L));

    IntStream.rangeClosed(6, 10).forEach(n -> publish("policy_data/stream", "/s", "v" + n));

    // Second connection, resuming from the position last seen on the first.
    var second =
        positionsOf(
            testKit
                .getSelfSseRouteTester()
                .receiveNFromOffset(path, 5, String.valueOf(first.get(first.size() - 1)), WAIT));

    assertThat(second).isEqualTo(List.of(6L, 7L, 8L, 9L, 10L));
    assertThat(second).doesNotContainAnyElementsOf(first);
  }

  @Test
  public void aReaderWithNoPositionStartsFromTheBeginning() {
    var path = "/channels/fresh_channel/stream";
    IntStream.rangeClosed(1, 3).forEach(n -> publish("fresh_channel/x", "/f", "v" + n));

    var seen = positionsOf(testKit.getSelfSseRouteTester().receiveFirstN(path, 3, WAIT));
    assertThat(seen).isEqualTo(List.of(1L, 2L, 3L));
  }

  @Test
  public void theStreamCarriesTheChangeItself() {
    var path = "/channels/payload_channel/stream";
    publish("payload_channel/x", "/p", "the-value");

    var events = testKit.getSelfSseRouteTester().receiveFirstN(path, 1, WAIT);
    assertThat(events).hasSize(1);
    assertThat(events.get(0).getId()).contains("1");
    assertThat(events.get(0).getData()).contains("the-value").contains("payload_channel");
  }
}
