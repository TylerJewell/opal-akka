package io.akka.opal.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §5 conformance for the surface: publishing, its answer, and reading a destination
 * back — against a real running service rather than an entity in isolation.
 */
public class PropagationEndpointIntegrationTest extends TestKitSupport {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static PropagationEndpoint.ChangeRequest change(String destination, String payload) {
    try {
      return new PropagationEndpoint.ChangeRequest(
          destination, List.of("policy_data"), JSON.readTree(payload), "test");
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private PropagationEndpoint.PublishResult publish(
      List<PropagationEndpoint.ChangeRequest> changes) {
    var response =
        httpClient
            .POST("/propagation/changes")
            .withRequestBody(new PropagationEndpoint.PublishRequest(changes))
            .responseBodyAs(PropagationEndpoint.PublishResult.class)
            .invoke();
    assertThat(response.status().isSuccess()).isTrue();
    return response.body();
  }

  @Test
  public void publishingAnswersWithTheIdentityTheDestinationAssigned() {
    var result = publish(List.of(change("/users", "{\"alice\":\"admin\"}")));
    assertThat(result.accepted()).hasSize(1);
    assertThat(result.accepted().get(0).id()).isEqualTo("/users#1");
    assertThat(result.accepted().get(0).sequence()).isEqualTo(1L);
    assertThat(result.batch()).isNotBlank();
  }

  @Test
  public void aBatchIsNumberedPerDestinationAndNotAcrossThem() {
    // Rule 8 and rule 14. Two destinations in one publish each start their own sequence at 1;
    // there is no number spanning them, and so no order between them either.
    var result =
        publish(List.of(change("/one", "{\"v\":1}"), change("/two", "{\"v\":2}")));
    assertThat(result.accepted()).hasSize(2);
    assertThat(result.accepted().get(0).id()).isEqualTo("/one#1");
    assertThat(result.accepted().get(1).id()).isEqualTo("/two#1");
    assertThat(result.accepted().get(0).sequence()).isEqualTo(1L);
    assertThat(result.accepted().get(1).sequence()).isEqualTo(1L);
  }

  @Test
  public void aChangeAddressedToNoTopicIsRefusedAtTheSurface() {
    var response =
        httpClient
            .POST("/propagation/changes")
            .withRequestBody(
                new PropagationEndpoint.PublishRequest(
                    List.of(
                        new PropagationEndpoint.ChangeRequest(
                            "/nowhere", List.of(), JSON.createObjectNode(), "test"))))
            .invoke();
    assertThat(response.status().isSuccess()).isFalse();
  }

  @Test
  public void publishingDoesNotWaitForASubscriberToApply() {
    // Rule 7. A subscriber exists and is addressed, but the answer names none of them and
    // arrives before any of them has been told.
    httpClient
        .POST("/propagation/subscribers/never-applies")
        .withRequestBody(new PropagationEndpoint.SubscribeRequest(List.of("policy_data")))
        .invoke();

    var result = publish(List.of(change("/independent", "{\"v\":1}")));
    assertThat(result.accepted().get(0).sequence()).isEqualTo(1L);
  }

  @Test
  public void aDestinationReadsBackItsSequenceAndItsValue() {
    publish(List.of(change("/readback", "{\"v\":1}")));
    publish(List.of(change("/readback", "{\"v\":2}")));

    var response =
        httpClient
            .GET("/propagation/destinations?path=/readback")
            .responseBodyAs(PropagationEndpoint.DestinationView.class)
            .invoke();
    assertThat(response.body().sequence()).isEqualTo(2L);
    assertThat(response.body().value().get("v").asInt()).isEqualTo(2);
    assertThat(response.body().oldestRetained()).isEqualTo(1L);
  }

  @Test
  public void theMissingSpanIsAskedForByNumber() {
    for (int i = 1; i <= 4; i++) {
      publish(List.of(change("/span", "{\"v\":" + i + "}")));
    }
    var response =
        httpClient
            .GET("/propagation/destinations/since?path=/span&after=2")
            .responseBodyAs(PropagationEndpoint.SpanView.class)
            .invoke();
    assertThat(response.body().complete()).isTrue();
    assertThat(response.body().changes()).hasSize(2);
    assertThat(response.body().changes().get(0).sequence()).isEqualTo(3L);
    assertThat(response.body().changes().get(1).sequence()).isEqualTo(4L);
  }

  @Test
  public void aPayloadMayBeABareNumber() {
    var result = publish(List.of(change("/scalar", "7")));
    assertThat(result.accepted().get(0).sequence()).isEqualTo(1L);
    var response =
        httpClient
            .GET("/propagation/destinations?path=/scalar")
            .responseBodyAs(PropagationEndpoint.DestinationView.class)
            .invoke();
    assertThat(response.body().value().asInt()).isEqualTo(7);
  }
}
