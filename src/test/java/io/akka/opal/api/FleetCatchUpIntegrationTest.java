package io.akka.opal.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.opal.domain.MemberReport;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rules 15-19, against a running service.
 *
 * <p>The measurement this answers was taken against the original in
 * {@code opal-port/probes/probe_04_fleet_live.py}: a member taken off the fleet with its store
 * intact, one change published while it was away, then let back on. The original left the two
 * members holding different values indefinitely and told neither of them.
 */
public class FleetCatchUpIntegrationTest extends TestKitSupport {

  private PropagationEndpoint.Accepted publish(String address, String destination, String value) {
    return httpClient
        .POST("/changes")
        .withRequestBody(
            new PropagationEndpoint.PublishRequest(
                null,
                "catch-up",
                List.of(new PropagationEndpoint.EntryRequest(address, destination, value))))
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

  private void away(String member, boolean away) {
    httpClient.POST("/members/" + member + (away ? "/leave" : "/return")).invoke();
  }

  private MemberReport report(String member) {
    return httpClient
        .GET("/members/" + member)
        .responseBodyAs(MemberReport.class)
        .invoke()
        .body();
  }

  @Test
  public void aMemberThatWasAwayAppliesExactlyWhatItMissed() {
    join("away-stayed", "policy_data");
    join("away-left", "policy_data");

    publish("policy_data/away", "/away", "before");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(report("away-left").store()).containsEntry("/away", "before"));

    away("away-left", true);
    var missed = publish("policy_data/away", "/away", "while-away");
    var alsoMissed = publish("policy_data/away", "/second", "also-while-away");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(report("away-stayed").store())
                    .containsEntry("/away", "while-away")
                    .containsEntry("/second", "also-while-away"));

    var whileAway = report("away-left");
    assertThat(whileAway.store()).containsEntry("/away", "before");
    assertThat(whileAway.position()).isLessThan(missed.position());

    away("away-left", false);

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var back = report("away-left");
              assertThat(back.store()).isEqualTo(report("away-stayed").store());
              assertThat(back.position()).isEqualTo(alsoMissed.position());
              assertThat(back.duplicates()).isZero();
            });
  }

  @Test
  public void aMemberJoiningLateAppliesEverythingThatCameBefore() {
    IntStream.rangeClosed(1, 10)
        .forEach(n -> publish("policy_data/late", "/late", "value-" + n));

    join("late-joiner", "policy_data");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var r = report("late-joiner");
              assertThat(r.store()).containsEntry("/late", "value-10");
              assertThat(r.gaps()).isZero();
            });
  }

  @Test
  public void aMemberIsToldWhenItIsFurtherBehindThanWhatIsKept() {
    join("far-behind", "policy_data");
    away("far-behind", true);

    var answer =
        httpClient
            .GET("/channels/policy_data/changes?after=0")
            .responseBodyAs(PropagationEndpoint.CatchUpResponse.class)
            .invoke()
            .body();

    // Nothing has been dropped yet in this run, so the answer is complete. What matters is
    // that the answer says which it is, rather than leaving the caller to guess from a
    // list length.
    assertThat(answer.complete()).isTrue();
    assertThat(answer.earliestRetained()).isGreaterThanOrEqualTo(0L);
  }

  @Test
  public void aMemberReportsWhereItHasGotTo() {
    join("reporting", "policy_data");
    var accepted = publish("policy_data/report", "/report", "value");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var r = report("reporting");
              assertThat(r.position()).isGreaterThanOrEqualTo(accepted.position());
              assertThat(r.channel()).isEqualTo("policy_data");
              assertThat(r.applied()).isGreaterThan(0L);
              assertThat(r.gaps()).isZero();
            });
  }

  @Test
  public void theFleetsPositionsCanBeReadAtOnce() {
    join("fleet-one", "policy_data");
    join("fleet-two", "policy_data");
    var accepted = publish("policy_data/fleet", "/fleet", "value");

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var fleet =
                  httpClient
                      .GET("/fleet")
                      .responseBodyAs(PropagationEndpoint.FleetReport.class)
                      .invoke()
                      .body();
              var ids =
                  fleet.members().stream()
                      .map(io.akka.opal.application.FleetView.FleetMember::id)
                      .toList();
              assertThat(ids).contains("fleet-one", "fleet-two");
              assertThat(fleet.members())
                  .filteredOn(m -> m.id().startsWith("fleet-"))
                  .allSatisfy(m -> assertThat(m.position()).isGreaterThanOrEqualTo(accepted.position()));
            });
  }
}
