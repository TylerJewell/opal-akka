package io.akka.opal.server.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-002 R113 to R118. */
class StatisticsTest {

  private record Published(String topic, Object data) {}

  private static OpalStatistics statistics(List<Published> published, int maxChannels, int workers) {
    return new OpalStatistics(
        "0.0.0", workers, maxChannels, 20, (topic, data) -> published.add(new Published(topic, data)));
  }

  /** R113: a client's own record is applied where it lands. */
  @Test
  void aClientRecordIsApplied() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 15, 1);
    statistics.addClient(
        Rpc.tree(
            Map.of("client_id", "c1", "rpc_id", "r1", "topics", List.of("policy:."))));
    assertEquals(1, statistics.state().clients().size());
    assertEquals(1, statistics.state().clients().get("c1").size());
    assertEquals("r1", statistics.state().clients().get("c1").get(0).rpc_id());
  }

  /** R114: a client may hold at most the configured number of channels. */
  @Test
  void aFurtherChannelBeyondTheLimitIsDropped() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 2, 1);
    for (int i = 0; i < 5; i++) {
      statistics.addClient(
          Rpc.tree(Map.of("client_id", "c1", "rpc_id", "r" + i, "topics", List.of("t"))));
    }
    assertEquals(2, statistics.state().clients().get("c1").size());
  }

  /** R115: a client left with no channels is removed entirely. */
  @Test
  void aClientWithNoChannelsLeftIsRemoved() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 15, 1);
    statistics.addClient(
        Rpc.tree(Map.of("client_id", "c1", "rpc_id", "r1", "topics", List.of("t"))));
    statistics.addClient(
        Rpc.tree(Map.of("client_id", "c1", "rpc_id", "r2", "topics", List.of("t"))));

    assertTrue(statistics.removeClient("r1"));
    assertEquals(1, statistics.state().clients().get("c1").size());
    assertTrue(statistics.removeClient("r2"));
    assertFalse(statistics.state().clients().containsKey("c1"));
  }

  /** An rpc id nobody registered is not an error; the broadcaster's own channel reaches here. */
  @Test
  void anUnknownChannelIsIgnored() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 15, 1);
    assertFalse(statistics.removeClient("never-seen"));
  }

  /** R116: a worker that already holds state ignores an answer to somebody else's question. */
  @Test
  void aWorkerHoldingStateIgnoresASyncAnswer() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 15, 1);
    statistics.addClient(
        Rpc.tree(Map.of("client_id", "mine", "rpc_id", "r1", "topics", List.of("t"))));
    statistics.receiveSyncedState(
        Rpc.tree(
            Map.of(
                "requesting_worker_id",
                statistics.workerId(),
                "clients",
                Map.of("theirs", List.of(Map.of("client_id", "theirs", "rpc_id", "r9", "topics",
                    List.of("t")))),
                "rpc_id_to_client_id",
                Map.of("r9", "theirs"))));
    assertTrue(statistics.state().clients().containsKey("mine"));
    assertFalse(statistics.state().clients().containsKey("theirs"));
  }

  /** R116: a worker with no state of its own applies the first answer it gets. */
  @Test
  void aWorkerWithNoStateAppliesTheFirstAnswer() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 15, 1);
    statistics.receiveSyncedState(
        Rpc.tree(
            Map.of(
                "requesting_worker_id",
                statistics.workerId(),
                "clients",
                Map.of("theirs", List.of(Map.of("client_id", "theirs", "rpc_id", "r9", "topics",
                    List.of("t")))),
                "rpc_id_to_client_id",
                Map.of("r9", "theirs"))));
    assertTrue(statistics.state().clients().containsKey("theirs"));
  }

  /** R117: a worker heard from joins the set, and one not heard from is dropped. */
  @Test
  void keepaliveAddsAndExpiresWorkers() {
    OpalStatistics statistics =
        new OpalStatistics("0.0.0", 1, 15, 0.001, (topic, data) -> {});
    statistics.receiveKeepalive(Rpc.tree(Map.of("worker_id", "other")));
    assertTrue(statistics.state().servers().contains("other"));

    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    statistics.expireOldServers();
    assertFalse(statistics.state().servers().contains("other"));
    assertTrue(statistics.state().servers().contains(statistics.workerId()));
  }

  /** R118: the replica count is the worker count divided by the workers per process. */
  @Test
  void theBriefCountDividesByTheWorkerCount() {
    OpalStatistics statistics = statistics(new ArrayList<>(), 15, 4);
    statistics.receiveKeepalive(Rpc.tree(Map.of("worker_id", "b")));
    statistics.receiveKeepalive(Rpc.tree(Map.of("worker_id", "c")));
    statistics.receiveKeepalive(Rpc.tree(Map.of("worker_id", "d")));
    assertEquals(1.0, statistics.stateBrief().server_count());
  }
}
