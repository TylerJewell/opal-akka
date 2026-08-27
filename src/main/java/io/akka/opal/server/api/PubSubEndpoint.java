package io.akka.opal.server.api;

import akka.NotUsed;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.Pair;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.WebSocket;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.http.HttpException;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.pubsub.ClientTracker;
import io.akka.opal.server.pubsub.RpcChannel;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The pub/sub channel — SPEC-002 R58 to R65.
 *
 * <p>One websocket per connection, carrying OPAL's own RPC frames in both directions. Outgoing
 * notifications do not arrive on the connection's own thread — they are produced by whatever
 * published them — so the socket's write side is a queue the notifier feeds and the stream drains.
 *
 * <p>A connection whose token does not verify is closed rather than answered, which the protocol
 * sees as an HTTP 403; there is no frame in the protocol for "you are not allowed to connect".
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class PubSubEndpoint extends AbstractHttpEndpoint {

  private static final Logger log = LoggerFactory.getLogger(PubSubEndpoint.class);

  private static final int OUTGOING_BUFFER = 256;

  private final ServerRuntime runtime;

  public PubSubEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  @WebSocket("/ws")
  public Flow<String, String, NotUsed> websocketRpcEndpoint() {
    if (!Role.isServer()) {
      throw HttpException.notFound();
    }
    Map<String, Object> claims;
    try {
      claims = Authn.requireLoggedIn(runtime.signer(), requestContext());
    } catch (Unauthorized e) {
      log.info("Closing connection, reason: Authentication failed");
      throw HttpException.forbidden();
    }

    Map<String, String> queryParams = requestContext().queryParams().toMap();
    // R217: a client that did not name itself is identified by where it connected from, so it
    // keeps one identity across reconnections instead of becoming a new client each time.
    String sourceHost = null;
    Integer sourcePort = null;
    java.util.Optional<akka.http.javadsl.model.HttpHeader> remote =
        requestContext().requestHeader("Remote-Address");
    if (remote.isPresent()) {
      String value = remote.get().value();
      int colon = value.lastIndexOf(':');
      if (colon > 0) {
        sourceHost = value.substring(0, colon);
        try {
          sourcePort = Integer.parseInt(value.substring(colon + 1));
        } catch (NumberFormatException e) {
          sourceHost = null;
        }
      }
    }
    ClientTracker.ClientInfo clientInfo =
        runtime.clientTracker().newClient(sourceHost, sourcePort, queryParams);

    return Flow.<String, String, Object>fromMaterializer(
            (materializer, attributes) -> {
              Pair<SourceQueueWithComplete<String>, Source<String, NotUsed>> outgoing =
                  Source.<String>queue(OUTGOING_BUFFER, OverflowStrategy.dropHead())
                      .preMaterialize(materializer);
              SourceQueueWithComplete<String> queue = outgoing.first();

              RpcChannel channel =
                  new RpcChannel(runtime.notifier(), frame -> queue.offer(frame));
              runtime.attachClaims(channel, claims);
              // R211: held so a resync after a backbone gap can close it. Completing the queue
              // ends the websocket, and the client reconnects and re-reads everything.
              runtime.connections().connect(channel.id(), queue::complete);
              // R218: with statistics on, the peer is asked for its own channel id and its
              // subscriptions are recorded under that rather than under this side's — which is
              // what makes one channel's statistics agree across two replicas.
              if (runtime.statistics() != null) {
                channel.requestOtherChannelId();
              }
              runtime
                  .notifier()
                  .registerSubscribeEvent(
                      (subscriberId, topics) -> {
                        if (subscriberId.equals(channel.subscriberId(false))) {
                          runtime.clientTracker().onSubscribe(clientInfo, topics);
                        }
                      });
              runtime
                  .notifier()
                  .registerUnsubscribeEvent(
                      (subscriberId, topics) -> {
                        if (subscriberId.equals(channel.subscriberId(false))) {
                          runtime.clientTracker().onUnsubscribe(clientInfo, topics);
                          // R325: an explicit unsubscribe takes the channel out of the fleet's
                          // statistics too, and says so to every replica. A client that
                          // unsubscribes without dropping its socket would otherwise be listed
                          // as connected for as long as it stayed open.
                          if (runtime.statistics() != null) {
                            runtime.statistics().removeClient(channel.id());
                            runtime.publish(
                                List.of(
                                    runtime
                                        .common()
                                        .getString("STATISTICS_REMOVE_CLIENT_CHANNEL")),
                                channel.id());
                          }
                        }
                      });

              Sink<String, java.util.concurrent.CompletionStage<akka.Done>> incoming =
                  Sink.<String>foreach(channel::onMessage)
                      .mapMaterializedValue(
                          done ->
                              done.whenComplete(
                                  (ignored, failure) -> onDisconnect(channel, clientInfo, queue)));

              return Flow.<String, String>fromSinkAndSourceCoupled(incoming, outgoing.second())
                  .mapMaterializedValue(ignored -> (Object) NotUsed.getInstance());
            })
        .mapMaterializedValue(ignored -> NotUsed.getInstance());
  }

  private void onDisconnect(
      RpcChannel channel, ClientTracker.ClientInfo clientInfo, SourceQueueWithComplete<String> queue) {
    runtime.connections().disconnect(channel.id());
    runtime.notifier().unsubscribe(channel.subscriberId(false), null);
    runtime.clientTracker().releaseClient(clientInfo.client_id);
    // R115: the fleet is told the channel went, so every replica drops its record of it.
    if (runtime.statistics() != null) {
      runtime.publish(
          List.of(runtime.common().getString("STATISTICS_REMOVE_CLIENT_CHANNEL")), channel.id());
    }
    queue.complete();
  }

  /** R65: the live map of client id to what is known about it. */
  @Get("/pubsub_client_info")
  public HttpResponse clientInfo() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(runtime.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      return Responses.ok(runtime.clientTracker().clients());
    });
  }
}
