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
import io.akka.opal.application.ChannelEntity;
import io.akka.opal.application.FleetDelivery;
import io.akka.opal.application.FleetView;
import io.akka.opal.application.MemberEntity;
import io.akka.opal.domain.Address;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.Entry;
import io.akka.opal.domain.MemberReport;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** The surface: publish a change, join or leave the fleet, read where a member is, follow a channel. */
@HttpEndpoint
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class PropagationEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;
  private final FleetDelivery delivery;

  public PropagationEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
    this.delivery = new FleetDelivery(componentClient);
  }

  public record EntryRequest(String address, String destination, String value) {}

  public record PublishRequest(String id, String reason, List<EntryRequest> entries) {}

  public record Accepted(String id, String channel, long position) {}

  public record JoinRequest(String channel, List<String> watching) {}

  public record CatchUpResponse(
      List<Change> changes, boolean complete, long earliestRetained, long currentPosition) {}

  public record FleetReport(List<FleetView.FleetMember> members) {}

  @Post("/changes")
  public Accepted publish(PublishRequest request) {
    if (request == null || request.entries() == null || request.entries().isEmpty()) {
      throw HttpException.badRequest("a change needs at least one entry");
    }
    var entries =
        request.entries().stream()
            .map(e -> new Entry(e.address(), e.destination(), e.value()))
            .toList();
    var channel = Address.of(entries.get(0).address()).channel();
    for (var entry : entries) {
      var of = Address.of(entry.address()).channel();
      if (!of.equals(channel)) {
        throw HttpException.badRequest(
            "a change belongs to one channel, and this one is addressed in both "
                + channel
                + " and "
                + of
                + " — publish them separately");
      }
    }

    var change =
        componentClient
            .forEventSourcedEntity(channel)
            .method(ChannelEntity::publish)
            .invoke(new ChannelEntity.Publish(request.id(), request.reason(), entries));
    return new Accepted(change.id(), change.channel(), change.position());
  }

  @Post("/members/{memberId}")
  public MemberReport join(String memberId, JoinRequest request) {
    if (request == null || request.channel() == null || request.channel().isBlank()) {
      throw HttpException.badRequest("a member joins one channel, named");
    }
    componentClient
        .forEventSourcedEntity(memberId)
        .method(MemberEntity::join)
        .invoke(new MemberEntity.Join(request.channel(), Set.copyOf(request.watching())));
    componentClient
        .forEventSourcedEntity(request.channel())
        .method(ChannelEntity::enrol)
        .invoke(memberId);
    return delivery.bringCurrent(memberId, request.channel());
  }

  @Get("/members/{memberId}")
  public MemberReport member(String memberId) {
    return componentClient
        .forEventSourcedEntity(memberId)
        .method(MemberEntity::report)
        .invoke();
  }

  /** Take a member off the fleet. Its store stays exactly as it is. */
  @Post("/members/{memberId}/leave")
  public MemberReport leave(String memberId) {
    return componentClient.forEventSourcedEntity(memberId).method(MemberEntity::leave).invoke();
  }

  /** Bring a member back, and hand it exactly what it missed, in order. */
  @Post("/members/{memberId}/return")
  public MemberReport comeBack(String memberId) {
    var back =
        componentClient.forEventSourcedEntity(memberId).method(MemberEntity::comeBack).invoke();
    return delivery.bringCurrent(memberId, back.channel());
  }

  @Get("/fleet")
  public FleetReport fleet() {
    return new FleetReport(
        componentClient.forView().method(FleetView::everyMember).invoke().members());
  }

  @Get("/channels/{channel}/changes")
  public CatchUpResponse changes(String channel) {
    var after = requestContext().queryParams().getLong("after").orElse(0L);
    var caughtUp =
        componentClient
            .forEventSourcedEntity(channel)
            .method(ChannelEntity::changesSince)
            .invoke(after);
    return new CatchUpResponse(
        caughtUp.changes(),
        caughtUp.complete(),
        caughtUp.earliestRetained(),
        caughtUp.currentPosition());
  }

  /**
   * Follow a channel. The stream carries each change once, in position order, and the position is
   * the event's identifier — so a reader that loses the connection sends back the last position it
   * saw and picks up from there.
   *
   * <p>What arrives is read from the channel itself, not from the live hint: the hint says
   * something was accepted, and the channel is then asked for everything after the last position
   * emitted. A hint that is lost costs a delay until the next one and never a missing change.
   */
  @Get("/channels/{channel}/stream")
  public HttpResponse follow(String channel) {
    var from = requestContext().lastSeenSseEventId().map(Long::parseLong).orElse(0L);

    // One materialisation per request, so this holds this reader's position and nobody else's.
    var emitted = new AtomicLong(from);

    Source<Long, NotUsed> hints =
        Source.single(from)
            .concat(
                componentClient
                    .forEventSourcedEntity(channel)
                    .notificationStream(ChannelEntity::accepted)
                    .source());

    Source<Change, NotUsed> changes =
        hints.flatMapConcat(
            hint ->
                // Asked for rather than waited for: blocking here would hold a thread that
                // every other reader of this stream shares.
                Source.completionStage(
                        componentClient
                            .forEventSourcedEntity(channel)
                            .method(ChannelEntity::changesSince)
                            .invokeAsync(emitted.get()))
                    .mapConcat(
                        caughtUp -> {
                          var fresh = caughtUp.changes();
                          if (!fresh.isEmpty()) {
                            emitted.set(fresh.get(fresh.size() - 1).position());
                          }
                          return fresh;
                        }));

    return HttpResponses.serverSentEvents(changes, change -> Long.toString(change.position()));
  }
}
