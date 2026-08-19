package io.akka.opal.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.opal.domain.MemberReport;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rules 6-13, against a running service.
 *
 * <p>This is the comparison the port exists for. The same shape was run against the original in
 * {@code opal-port/probes/probe_04_fleet_live.py}: several producers changing one destination at
 * once, and two members watching different names.
 */
public class FleetOrderingIntegrationTest extends TestKitSupport {

  private static final int PRODUCERS = 4;
  private static final int PER_PRODUCER = 25;

  private PropagationEndpoint.Accepted publish(String address, String destination, String value) {
    return httpClient
        .POST("/changes")
        .withRequestBody(
            new PropagationEndpoint.PublishRequest(
                null, "integration", List.of(new PropagationEndpoint.EntryRequest(address, destination, value))))
        .responseBodyAs(PropagationEndpoint.Accepted.class)
        .invoke()
        .body();
  }

  private void join(String member, String... watching) {
    httpClient
        .POST("/members/" + member)
        .withRequestBody(new PropagationEndpoint.JoinRequest("policy_data", List.of(watching)))
        .invoke();
  }

  private MemberReport report(String member) {
    return httpClient
        .GET("/members/" + member)
        .responseBodyAs(MemberReport.class)
        .invoke()
        .body();
  }

  @Test
  public void everyChangeGetsItsOwnPositionEvenWhenProducersPublishAtOnce() throws Exception {
    join("ordering-a", "policy_data");
    ExecutorService pool = Executors.newFixedThreadPool(PRODUCERS);
    try {
      List<Callable<List<Long>>> work =
          IntStream.range(0, PRODUCERS)
              .mapToObj(
                  p ->
                      (Callable<List<Long>>)
                          () ->
                              IntStream.rangeClosed(1, PER_PRODUCER)
                                  .mapToObj(
                                      n ->
                                          publish(
                                                  "policy_data/burst",
                                                  "/burst",
                                                  "producer" + p + "-" + n)
                                              .position())
                                  .toList())
              .toList();

      var positions = pool.invokeAll(work).stream().flatMap(f -> {
        try {
          return f.get().stream();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).sorted().toList();

      var expected =
          IntStream.rangeClosed(1, PRODUCERS * PER_PRODUCER).mapToObj(Long::valueOf).toList();
      assertThat(positions).isEqualTo(expected);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void twoMembersWatchingTheSameNamesEndUpHoldingTheSameThing() {
    join("agree-a", "policy_data");
    join("agree-b", "policy_data");

    IntStream.rangeClosed(1, 20)
        .forEach(n -> publish("policy_data/agree", "/agree", "value-" + n));

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var a = report("agree-a");
              var b = report("agree-b");
              assertThat(a.store()).isEqualTo(b.store());
              assertThat(a.store().get("/agree")).isEqualTo("value-20");
              assertThat(a.position()).isEqualTo(b.position());
            });
  }

  @Test
  public void aChangeReachesAMemberWatchingAboveItAndNotOneWatchingBelowIt() {
    join("reach-above", "policy_data");
    join("reach-below", "policy_data/reach/deeper");

    publish("policy_data/reach", "/reach", "delivered");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(report("reach-above").store()).containsEntry("/reach", "delivered"));

    assertThat(report("reach-below").store()).doesNotContainKey("/reach");
  }

  @Test
  public void aMemberThatWatchesNothingRelevantStillKeepsUpWithThePosition() {
    join("silent", "policy_data/nothing/here");
    var accepted = publish("policy_data/elsewhere", "/elsewhere", "not for silent");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var r = report("silent");
              assertThat(r.position()).isGreaterThanOrEqualTo(accepted.position());
              assertThat(r.store()).doesNotContainKey("/elsewhere");
            });
  }

  @Test
  public void aMemberHandedAChangeAheadOfItRecordsItAndThenClosesIt() {
    // A member joining while changes are being published can be handed one that is further
    // ahead than it has reached. The count is what a gap costs; being left behind is not,
    // because the same delivery brings the member current and hands the change over again.
    join("gap-closer", "policy_data");
    var ahead = publish("policy_data/gap", "/gap", "ahead");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var r = report("gap-closer");
              assertThat(r.position()).isGreaterThanOrEqualTo(ahead.position());
              assertThat(r.store()).containsEntry("/gap", "ahead");
            });
  }

  @Test
  public void aChangeSpanningTwoChannelsIsRefused() {
    var response =
        httpClient
            .POST("/changes")
            .withRequestBody(
                new PropagationEndpoint.PublishRequest(
                    null,
                    "integration",
                    List.of(
                        new PropagationEndpoint.EntryRequest("policy_data/a", "/a", "1"),
                        new PropagationEndpoint.EntryRequest("other_root/b", "/b", "2"))))
            .invoke();

    assertThat(response.status().intValue()).isEqualTo(400);
  }
}
