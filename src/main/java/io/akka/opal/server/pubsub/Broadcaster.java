package io.akka.opal.server.pubsub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * The backbone that carries publications between server replicas — SPEC-002 R66 and R200–R209.
 *
 * <p>Three transports, selected by the scheme of {@code BROADCAST_URI}. All three carry the same
 * payload on a channel named by {@code BROADCAST_CHANNEL_NAME}, which is what makes a replica of
 * this rebuild and a replica of the original interoperable on one backbone.
 *
 * <p>None of them stores anything, so a replica that was disconnected when a publication went
 * past has missed it. That is the whole reason for the rest of this interface: a reader that
 * reconnects rather than dying, a record of whether it is currently in a gap, a buffer of what
 * could not be sent during one, and a hook that fires once the gap closes.
 */
public interface Broadcaster extends AutoCloseable {

  /** What travels on the backbone: who sent it, which topics, and the data. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  record BroadcastNotification(String notifier_id, List<String> topics, Object data) {}

  /** Called for each notification read off the backbone that this replica did not send. */
  interface Reader {
    void onNotification(BroadcastNotification notification);
  }

  /**
   * How a dropped backbone is handled — SPEC-002 R200.
   *
   * @param reconnect whether the reader reconnects at all, or ends at the first drop
   * @param maxRetries consecutive failed attempts before giving up; zero retries forever
   * @param backoffMinSeconds the first wait, doubling from there
   * @param backoffMaxSeconds the ceiling on that wait
   * @param replayBufferSize how many publications to keep while the backbone is away
   * @param resyncSettleSeconds how long to let peers re-subscribe before replaying
   * @param resyncOnReconnect whether the recovery hook fires after a gap
   */
  record Resilience(
      boolean reconnect,
      int maxRetries,
      double backoffMinSeconds,
      double backoffMaxSeconds,
      int replayBufferSize,
      double resyncSettleSeconds,
      boolean resyncOnReconnect) {

    public static Resilience off() {
      return new Resilience(false, 0, 1, 30, 0, 0, false);
    }
  }

  void start(Reader reader);

  void publish(BroadcastNotification notification);

  /** This replica's own id, so it can skip what it published itself. */
  String id();

  /** R200: how this backbone behaves when it drops. Set before {@link #start}. */
  void configureResilience(Resilience resilience);

  /**
   * R205: whether the reader can still serve the clients that depend on it.
   *
   * <p>A reader part-way through a reconnection is healthy: the whole point of reconnecting is
   * that a transient drop is not a fault. It is unhealthy only when the reader is not running at
   * all or has given up, which is exactly when a client's publications have nowhere to arrive
   * from.
   */
  boolean isReaderHealthy();

  /**
   * R206: whether the backbone had a subscription, lost it, and has not got it back.
   *
   * <p>Not the same as "not connected". A reader that has never connected is not in a gap,
   * because the recovery a gap relies on only fires for a reconnection that follows an
   * established session — freezing publications in that window would drop them with nothing to
   * reconcile them afterwards.
   */
  boolean isInBackboneGap();

  /** R207: how many gaps there have been, so two consecutive ones are told apart. */
  int gapGeneration();

  /** R208: run after a gap closes and the buffer has been replayed. */
  void setOnReconnect(Runnable onReconnect);

  /** R209: run when the reader gives up, so the process can be replaced. */
  void setOnGiveUp(Runnable onGiveUp);

  /**
   * R337: run when the reader loop has ended for any reason while the process is still up.
   *
   * <p>A worker that is no longer reading the backbone cannot keep fleet statistics that
   * anybody should believe, so the server ends the process and lets a supervisor start a
   * replacement that reads again.
   */
  void setOnReaderEnded(Runnable onReaderEnded);

  @Override
  void close();
}
