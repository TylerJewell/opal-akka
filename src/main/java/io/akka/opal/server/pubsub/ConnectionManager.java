package io.akka.opal.server.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client connections this replica is holding — SPEC-002 R210 and R211.
 *
 * <p>Two things use it. Disconnecting is idempotent, because the same socket can be reported gone
 * twice and the second report is not an error. And every connection can be closed on purpose,
 * which is how a replica that has just come back from a backbone gap makes its own clients
 * re-read everything: a client reconnects by itself and re-runs its reconciliation, so closing
 * the socket is the whole of the instruction.
 */
public final class ConnectionManager {

  private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

  /** How a connection is closed, which is whatever the transport gave the endpoint. */
  public interface Connection {
    void close();
  }

  private final Map<String, Connection> active = new ConcurrentHashMap<>();

  public void connect(String id, Connection connection) {
    active.put(id, connection);
  }

  /** R210: reporting the same connection gone twice is not an error. */
  public void disconnect(String id) {
    if (active.remove(id) == null) {
      log.debug("Ignoring duplicate websocket disconnect");
    }
  }

  public int count() {
    return active.size();
  }

  /**
   * R211: closes every connection, spaced out, and answers how many it closed.
   *
   * <p>Spaced out because a replica holding a thousand clients that all reconnect in the same
   * millisecond has replaced one problem with another. The wait is between closes and not after
   * the last one.
   */
  public int closeAllStaggered(double minIntervalSeconds, double maxIntervalSeconds) {
    List<Map.Entry<String, Connection>> connections = new ArrayList<>(active.entrySet());
    if (connections.isEmpty()) {
      return 0;
    }
    log.info(
        "Resync: closing {} client connection(s) to trigger client-side reconciliation",
        connections.size());
    int closed = 0;
    for (int index = 0; index < connections.size(); index++) {
      Map.Entry<String, Connection> entry = connections.get(index);
      try {
        entry.getValue().close();
        closed++;
      } catch (RuntimeException e) {
        log.warn("Error closing a client websocket during resync: {}", e.toString());
      }
      active.remove(entry.getKey());
      if (maxIntervalSeconds > 0 && index < connections.size() - 1) {
        try {
          Thread.sleep(
              (long)
                  (ThreadLocalRandom.current().nextDouble(minIntervalSeconds, maxIntervalSeconds)
                      * 1000));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return closed;
        }
      }
    }
    return closed;
  }
}
