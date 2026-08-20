package io.akka.opal.api;

import akka.NotUsed;
import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.http.HttpException;
import akka.javasdk.http.HttpResponses;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.opal.application.CursorEntity;
import io.akka.opal.application.CursorsByDestinationView;
import io.akka.opal.application.DestinationEntity;
import io.akka.opal.application.SubscriptionEntity;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.CursorState;
import io.akka.opal.domain.Gap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The surface of the capability — the four things the source exposes, plus the two its
 * subscribers have no way to ask for.
 *
 * <p>A destination is a path with slashes in it, so it travels as a query parameter rather
 * than as part of the route. That keeps {@code /users/keys} one destination instead of two
 * route segments.
 */
@HttpEndpoint("/propagation")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class PropagationEndpoint extends AbstractHttpEndpoint {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final ComponentClient componentClient;

  public PropagationEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  /** One change in a publish. The payload is any JSON value — rule 9. */
  public record ChangeRequest(String destination, List<String> topics, JsonNode payload,
      String reason) {}

  /** Several changes published together — rule 8. Each goes to its own destination. */
  public record PublishRequest(List<ChangeRequest> changes) {}

  public record Accepted(String id, String destination, long sequence) {}

  public record PublishResult(String batch, List<Accepted> accepted) {}

  public record SubscribeRequest(List<String> topics) {}

  public record ChangeView(String id, String destination, long sequence, List<String> topics,
      JsonNode payload, String reason, String batch) {}

  /**
   * @param oldestRetained the earliest change still available by number; below it a subscriber
   *     takes the value instead
   */
  public record DestinationView(String destination, long sequence, JsonNode value,
      long oldestRetained) {}

  public record CursorView(String subscriber, String destination, long applied, List<Gap> gaps,
      JsonNode value) {}

  public record SpanView(boolean complete, long sequence, JsonNode value,
      List<ChangeView> changes) {}

  /** How far each subscriber is from the front of a destination — rule 19. */
  public record Convergence(String destination, long sequence, boolean converged,
      List<Standing> subscribers) {}

  public record Standing(String subscriber, long applied, long behind, boolean hasGap) {}

  /**
   * Rules 4 to 9. Answers with the identity every accepted change was given, so the publisher
   * can say afterwards which change it published — which the source cannot (question-log
   * row 23).
   */
  @Post("/changes")
  public HttpResponse publish(PublishRequest request) {
    String batch = UUID.randomUUID().toString();
    List<Accepted> accepted = new ArrayList<>();

    for (ChangeRequest change : request.changes()) {
      Change stored =
          componentClient
              .forEventSourcedEntity(change.destination())
              .method(DestinationEntity::publish)
              .invoke(
                  new DestinationEntity.Publish(
                      change.topics(), asText(change.payload()), change.reason(), batch));
      accepted.add(new Accepted(stored.id(), stored.destination(), stored.sequence()));
    }
    return HttpResponses.created(new PublishResult(batch, accepted));
  }

  @Post("/subscribers/{subscriber}")
  public HttpResponse subscribe(String subscriber, SubscribeRequest request) {
    componentClient
        .forKeyValueEntity(subscriber)
        .method(SubscriptionEntity::subscribe)
        .invoke(new SubscriptionEntity.Subscribe(request.topics()));
    return HttpResponses.ok();
  }

  @Get("/destinations")
  public DestinationView destination() {
    String path = required("path");
    DestinationEntity.Summary summary =
        componentClient
            .forEventSourcedEntity(path)
            .method(DestinationEntity::summary)
            .invoke();
    return new DestinationView(
        summary.destination(), summary.sequence(), asJson(summary.value()),
        summary.oldestRetained());
  }

  /** Rules 16 and 18: what a subscriber is missing, by number. */
  @Get("/destinations/since")
  public SpanView since() {
    String path = required("path");
    long after = requestContext().queryParams().getLong("after").orElse(0L);
    DestinationEntity.Span span =
        componentClient
            .forEventSourcedEntity(path)
            .method(DestinationEntity::since)
            .invoke(after);
    return new SpanView(
        span.complete(),
        span.sequence(),
        asJson(span.value()),
        span.changes().stream().map(PropagationEndpoint::toApi).toList());
  }

  @Get("/cursors")
  public CursorView cursor() {
    String subscriber = required("subscriber");
    String destination = required("destination");
    CursorState state =
        componentClient
            .forEventSourcedEntity(CursorEntity.Id.of(subscriber, destination))
            .method(CursorEntity::read)
            .invoke();
    return new CursorView(
        state.subscriber(), state.destination(), state.applied(), state.gaps(),
        asJson(state.value()));
  }

  /** Rule 19 — the question the source has no way to ask. */
  @Get("/convergence")
  public Convergence convergence() {
    String destination = required("destination");
    DestinationEntity.Summary front =
        componentClient
            .forEventSourcedEntity(destination)
            .method(DestinationEntity::summary)
            .invoke();
    CursorsByDestinationView.Cursors cursors =
        componentClient
            .forView()
            .method(CursorsByDestinationView::byDestination)
            .invoke(destination);

    List<Standing> standing =
        cursors.items().stream()
            .map(
                row ->
                    new Standing(
                        row.subscriber(),
                        row.applied(),
                        front.sequence() - row.applied(),
                        row.hasGap()))
            .toList();
    boolean converged = standing.stream().allMatch(s -> s.behind() == 0 && !s.hasGap());
    return new Convergence(destination, front.sequence(), converged, standing);
  }

  /** A live view of a destination. Not the delivery path — see {@code DestinationEntity}. */
  @Get("/watch")
  public HttpResponse watch() {
    String destination = required("destination");
    Source<ChangeView, NotUsed> source =
        componentClient
            .forEventSourcedEntity(destination)
            .notificationStream(DestinationEntity::updates)
            .source()
            .map(PropagationEndpoint::toApi);
    return HttpResponses.serverSentEvents(source);
  }

  /**
   * A destination is a path with slashes in it, so it arrives as a query parameter. Missing
   * is a bad request, not an empty string — an empty destination would be a real entity id
   * and would quietly answer about the wrong thing.
   */
  private String required(String name) {
    return requestContext()
        .queryParams()
        .getString(name)
        .filter(value -> !value.isBlank())
        .orElseThrow(() -> HttpException.badRequest("missing query parameter: " + name));
  }

  private static ChangeView toApi(Change change) {
    return new ChangeView(
        change.id(),
        change.destination(),
        change.sequence(),
        change.topics(),
        asJson(Optional.of(change.payload())),
        change.reason(),
        change.batch());
  }

  private static String asText(JsonNode payload) {
    return payload == null ? null : payload.toString();
  }

  /**
   * A stored payload is JSON by construction — it was parsed on the way in. The message says
   * only where the unparseable one is, never what it held: a payload is the caller's own data
   * and an exception message reaches logs and error responses alike.
   */
  private static JsonNode asJson(Optional<String> payload) {
    if (payload.isEmpty()) {
      return null;
    }
    try {
      return JSON.readTree(payload.get());
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalStateException("a stored payload could not be read back as JSON", e);
    }
  }
}
