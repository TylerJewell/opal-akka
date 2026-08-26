package io.akka.opal.server.stats;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.server.pubsub.Rpc;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Who is connected to the fleet, kept in step across replicas — SPEC-002 R113 to R118.
 *
 * <p>Every replica keeps its own copy and they converge through the same pub/sub channel the
 * fleet already has: a starting replica asks, one that holds state answers after a random wait,
 * and the first answer wins. The random wait is what stops twenty replicas all answering one
 * question at once.
 */
public final class OpalStatistics {

  private static final Logger log = LoggerFactory.getLogger(OpalStatistics.class);

  private static final double MIN_TIME_TO_WAIT_SECONDS = 0.001;
  private static final double MAX_TIME_TO_WAIT_SECONDS = 5;

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record ChannelStats(String rpc_id, String client_id, List<String> topics) {}

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record ServerStats(
      String uptime, String version, Map<String, List<ChannelStats>> clients, Set<String> servers) {}

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record ServerStatsBrief(
      String uptime, String version, int client_count, double server_count) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SyncRequest(String requesting_worker_id) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SyncResponse(
      String requesting_worker_id,
      Map<String, List<ChannelStats>> clients,
      Map<String, String> rpc_id_to_client_id) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record ServerKeepalive(String worker_id) {}

  private final String uptime = Instant.now().toString();
  private final String version;
  private final String workerId = UUID.randomUUID().toString().replace("-", "");
  private final int workersCount;
  private final int maxChannelsPerClient;
  private final double keepaliveTimeoutSeconds;

  private final Map<String, List<ChannelStats>> clients = new LinkedHashMap<>();
  private final Map<String, String> rpcIdToClientId = new LinkedHashMap<>();
  private final Set<String> servers = new LinkedHashSet<>();
  private final Map<String, Instant> seenServers = new LinkedHashMap<>();
  private final Set<String> receivedSyncMessages = new LinkedHashSet<>();

  private final BiConsumer<String, Object> publish;
  private volatile boolean syncedAfterWakeup;
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> keepaliveTask;

  public OpalStatistics(
      String version,
      int workersCount,
      int maxChannelsPerClient,
      double keepaliveTimeoutSeconds,
      BiConsumer<String, Object> publish) {
    this.version = version;
    this.workersCount = Math.max(1, workersCount);
    this.maxChannelsPerClient = maxChannelsPerClient;
    this.keepaliveTimeoutSeconds = keepaliveTimeoutSeconds;
    this.publish = publish;
    this.servers.add(workerId);
  }

  public String workerId() {
    return workerId;
  }

  public synchronized ServerStats state() {
    return new ServerStats(
        uptime, version, new LinkedHashMap<>(clients), new LinkedHashSet<>(servers));
  }

  /** R118: the replica count is the number of known workers divided by the workers per process. */
  public synchronized ServerStatsBrief stateBrief() {
    return new ServerStatsBrief(
        uptime, version, clients.size(), (double) servers.size() / workersCount);
  }

  /** R117: a worker not heard from inside the timeout is dropped from the set. */
  synchronized void expireOldServers() {
    Instant now = Instant.now();
    Map<String, Instant> stillAlive = new LinkedHashMap<>();
    seenServers.forEach(
        (serverId, lastSeen) -> {
          if (java.time.Duration.between(lastSeen, now).toMillis() / 1000.0
              < keepaliveTimeoutSeconds) {
            stillAlive.put(serverId, lastSeen);
          }
        });
    seenServers.clear();
    seenServers.putAll(stillAlive);
    servers.clear();
    servers.add(workerId);
    servers.addAll(seenServers.keySet());
  }

  public void startKeepalive(String keepaliveChannel) {
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-stats-keepalive");
              thread.setDaemon(true);
              return thread;
            });
    long periodMillis = (long) (keepaliveTimeoutSeconds / 2 * 1000);
    keepaliveTask =
        scheduler.scheduleWithFixedDelay(
            () -> {
              try {
                expireOldServers();
                publish.accept(keepaliveChannel, new ServerKeepalive(workerId));
              } catch (Exception e) {
                log.warn("Statistics: periodic server keepalive failed", e);
              }
            },
            0,
            Math.max(1, periodMillis),
            TimeUnit.MILLISECONDS);
  }

  public synchronized void stop() {
    if (keepaliveTask != null) {
      keepaliveTask.cancel(true);
      keepaliveTask = null;
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
  }

  /**
   * R215: a starting replica asks the fleet for what it already knows.
   *
   * <p>Without this a replica answers other replicas' questions and never asks one, so it starts
   * empty and stays empty until every client happens to reconnect through it — and its
   * {@code /statistics} answer disagrees with every peer's for as long as that takes. The wait
   * first is for the backbone reader to be listening; a request published before it is subscribed
   * reaches nobody, and nothing asks again.
   */
  public void requestFleetState(String wakeupChannel) {
    Thread asking =
        new Thread(
            () -> {
              try {
                Thread.sleep((long) (SLEEP_BEFORE_WAKEUP_SECONDS * 1000));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              log.info("Asking the fleet for its statistics state");
              publish.accept(wakeupChannel, new SyncRequest(workerId));
            },
            "opal-stats-wakeup");
    asking.setDaemon(true);
    asking.start();
  }

  /** How long to let the backbone reader subscribe before asking on it. */
  static final double SLEEP_BEFORE_WAKEUP_SECONDS = 2.0;

  /** R116: answer a starting worker's request, unless somebody else already did. */
  public void receiveWakeup(JsonNode message, String stateSyncChannel) {
    SyncRequest request;
    try {
      request = Rpc.MAPPER.treeToValue(message, SyncRequest.class);
    } catch (Exception e) {
      log.warn("Got invalid statistics sync request from another server, error: {}", e.toString());
      return;
    }
    if (workerId.equals(request.requesting_worker_id())) {
      return;
    }
    boolean haveState;
    synchronized (this) {
      haveState = !clients.isEmpty();
    }
    if (!haveState) {
      return;
    }
    double wait =
        MIN_TIME_TO_WAIT_SECONDS
            + ThreadLocalRandom.current().nextDouble()
                * (MAX_TIME_TO_WAIT_SECONDS - MIN_TIME_TO_WAIT_SECONDS);
    try {
      Thread.sleep((long) (wait * 1000));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    synchronized (this) {
      if (receivedSyncMessages.contains(request.requesting_worker_id())) {
        return;
      }
      log.info("[{}] respond with my own stats", request.requesting_worker_id());
      publish.accept(
          stateSyncChannel,
          new SyncResponse(
              request.requesting_worker_id(),
              new LinkedHashMap<>(clients),
              new LinkedHashMap<>(rpcIdToClientId)));
    }
  }

  /** R116's other half: a worker that already holds state ignores an answer. */
  public synchronized void receiveSyncedState(JsonNode message) {
    SyncResponse response;
    try {
      response = Rpc.MAPPER.treeToValue(message, SyncResponse.class);
    } catch (Exception e) {
      log.warn("Got invalid statistics sync response from another server, error: {}", e.toString());
      return;
    }
    receivedSyncMessages.add(response.requesting_worker_id());
    if (clients.isEmpty() && !syncedAfterWakeup) {
      log.info("[{}] applying server stats", response.requesting_worker_id());
      clients.clear();
      if (response.clients() != null) {
        clients.putAll(response.clients());
      }
      rpcIdToClientId.clear();
      if (response.rpc_id_to_client_id() != null) {
        rpcIdToClientId.putAll(response.rpc_id_to_client_id());
      }
      syncedAfterWakeup = true;
    }
  }

  public synchronized void receiveKeepalive(JsonNode message) {
    String otherWorkerId = message.path("worker_id").asText(null);
    if (otherWorkerId == null) {
      return;
    }
    seenServers.put(otherWorkerId, Instant.now());
    servers.add(otherWorkerId);
  }

  /** R114: a client may hold at most the configured number of channel records. */
  public synchronized void addClient(JsonNode message) {
    ChannelStats stats;
    try {
      stats = Rpc.MAPPER.treeToValue(message, ChannelStats.class);
    } catch (Exception e) {
      log.warn("Got invalid statistics message from client, error: {}", e.toString());
      return;
    }
    if (stats.client_id() == null || stats.rpc_id() == null) {
      log.warn("Got invalid statistics message from client, error: missing id");
      return;
    }
    log.info(
        "Set client statistics {} on channel {} with {}",
        stats.client_id(),
        stats.rpc_id(),
        String.join(", ", stats.topics() == null ? List.of() : stats.topics()));
    rpcIdToClientId.put(stats.rpc_id(), stats.client_id());
    List<ChannelStats> existing = clients.get(stats.client_id());
    if (existing == null) {
      List<ChannelStats> created = new ArrayList<>();
      created.add(stats);
      clients.put(stats.client_id(), created);
      return;
    }
    if (existing.size() < maxChannelsPerClient) {
      existing.add(stats);
    } else {
      log.warn("Client '{}' reached the maximum number of open RPC channels", stats.client_id());
    }
  }

  /** R115: a client left with no channels is removed entirely. */
  public synchronized boolean removeClient(String rpcId) {
    String clientId = rpcIdToClientId.get(rpcId);
    if (clientId == null) {
      log.debug("Statistics.remove_client() got unknown rpc id: {} (probably broadcaster)", rpcId);
      return false;
    }
    List<ChannelStats> channels = clients.get(clientId);
    if (channels != null) {
      channels.removeIf(stats -> stats.rpc_id().equals(rpcId));
      if (channels.isEmpty()) {
        clients.remove(clientId);
      }
    }
    rpcIdToClientId.remove(rpcId);
    return true;
  }
}
