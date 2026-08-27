package io.akka.opal.server.pubsub;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * The three backbones {@code BROADCAST_URI} can name, built from its scheme.
 *
 * <p>Each is a thin relay rather than a queue: a publication is written once and every replica
 * listening reads it. Nothing is stored, which is why a replica that was disconnected when a
 * publication went past has missed it — the reconnect resync exists for exactly that gap.
 *
 * <p>The reconnection, the gap bookkeeping and the replay buffer are the same for all three, so
 * they live in {@link Base} and each transport supplies only one thing: a session that connects,
 * subscribes and reads until the connection ends.
 */
public final class Broadcasters {

  private static final Logger log = LoggerFactory.getLogger(Broadcasters.class);

  private Broadcasters() {}

  public static Broadcaster forUri(String uri, String channel) {
    if (uri == null) {
      return null;
    }
    int colon = uri.indexOf(':');
    if (colon <= 0) {
      throw new IllegalArgumentException("BROADCAST_URI has no scheme: " + uri);
    }
    String scheme = uri.substring(0, colon);
    return switch (scheme) {
      case "redis", "rediss" -> new RedisBroadcaster(uri, channel);
      case "postgres", "postgresql" -> new PostgresBroadcaster(uri, channel);
      case "kafka" -> new KafkaBroadcaster(uri, channel);
      case "memory" -> new MemoryBroadcaster(channel);
      default -> throw new IllegalArgumentException("unsupported BROADCAST_URI scheme: " + scheme);
    };
  }

  /**
   * Everything a backbone does that is not the transport itself.
   *
   * <p>The reader loop, the wait between attempts, the count of gaps, the buffer of what could
   * not be sent during one, and the recovery that runs after one closes.
   */
  abstract static class Base implements Broadcaster {

    private final String id = EventNotifier.generateId();
    final String channel;
    final AtomicBoolean running = new AtomicBoolean();

    private Resilience resilience = Resilience.off();
    private Runnable onReconnect;
    private Runnable onGiveUp;
    private Runnable onReaderEnded;

    private Thread readerThread;
    volatile Reader reader;
    private volatile boolean subscribed;
    private volatile boolean everSubscribed;
    private volatile boolean gaveUp;
    private volatile int gapGeneration;

    private final Deque<BroadcastNotification> outbound = new ArrayDeque<>();
    private final Object bufferLock = new Object();
    private final Object recoveryLock = new Object();
    private Thread recoveryThread;
    private boolean recoveryRerunRequested;

    Base(String channel) {
      this.channel = channel;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public void configureResilience(Resilience settings) {
      this.resilience = settings == null ? Resilience.off() : settings;
    }

    Resilience resilience() {
      return resilience;
    }

    @Override
    public void setOnReconnect(Runnable callback) {
      this.onReconnect = callback;
    }

    @Override
    public void setOnGiveUp(Runnable callback) {
      this.onGiveUp = callback;
    }

    @Override
    public void setOnReaderEnded(Runnable callback) {
      this.onReaderEnded = callback;
    }

    @Override
    public boolean isReaderHealthy() {
      // R323: nothing listening is idleness, not a fault. The source answers healthy while its
      // listener count is zero, because a reader that is not there is only a problem for
      // somebody waiting on it — and a worker reported unhealthy for being idle is taken out of
      // a load balancer for doing nothing wrong.
      if (readerThread == null) {
        return true;
      }
      return !gaveUp && readerThread.isAlive();
    }

    @Override
    public boolean isInBackboneGap() {
      return isReaderHealthy() && everSubscribed && !subscribed;
    }

    @Override
    public int gapGeneration() {
      return gapGeneration;
    }

    /**
     * Connects, subscribes and reads until the connection ends.
     *
     * <p>Returning normally means the subscription ended; throwing means it could not be
     * established, and the two are counted differently. {@code onSubscribed} is run once the
     * subscription exists, and {@code onEvent} once per notification read.
     */
    abstract void session(Reader reader, Runnable onSubscribed, Runnable onEvent) throws Exception;

    /** Sends one notification on the backbone, or throws if it cannot. */
    abstract void send(BroadcastNotification notification) throws Exception;

    /** Closes whatever the transport holds open. */
    abstract void closeTransport();

    @Override
    public final void start(Reader reader) {
      if (readerThread != null) {
        return;
      }
      // Held before the thread starts: an in-process transport delivers on the publishing
      // thread, so a publication made immediately after start must already know where to go.
      this.reader = reader;
      running.set(true);
      readerThread =
          new Thread(
              () -> {
                try {
                  loop(reader);
                } finally {
                  fireReaderEnded();
                }
              },
              "opal-broadcast-" + getClass().getSimpleName());
      readerThread.setDaemon(true);
      readerThread.start();
    }

    /**
     * R201: the reader loop, which reconnects rather than ending at the first drop.
     *
     * <p>A session that ends without ever delivering anything is a failed attempt rather than a
     * closed session, and only a delivering one resets the attempt counter — otherwise a
     * connect-and-immediately-close loop would never reach the give-up ceiling.
     */
    private void loop(Reader reader) {
      int attempt = 0;
      boolean hadPriorConnection = false;
      while (running.get()) {
        boolean[] sustained = {false};
        boolean[] connected = {false};
        boolean prior = hadPriorConnection;
        try {
          session(
              reader,
              () -> {
                subscribed = true;
                everSubscribed = true;
                connected[0] = true;
                log.info("Broadcaster listener connected to channel '{}'", channel);
                if (prior) {
                  scheduleGapRecovery();
                }
              },
              () -> sustained[0] = true);
          if (connected[0]) {
            hadPriorConnection = true;
          }
          if (sustained[0]) {
            attempt = 0;
            log.warn("Broadcast subscriber ended (backbone connection closed); reconnecting");
          } else {
            attempt++;
            log.warn(
                "Broadcast subscriber ended immediately without sustaining (attempt {}); "
                    + "treating as a failed reconnect",
                attempt);
            if (gaveUp(attempt)) {
              return;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          attempt++;
          if (running.get()) {
            log.error("Broadcaster listener error (attempt {}): {}", attempt, e.toString());
          }
          if (gaveUp(attempt)) {
            return;
          }
        } finally {
          if (subscribed) {
            gapGeneration++;
          }
          subscribed = false;
        }
        if (!running.get() || !resilience.reconnect()) {
          return;
        }
        try {
          Thread.sleep((long) (backoffSeconds(attempt) * 1000));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    /** R202: whether the attempts are exhausted. Zero retries means never give up. */
    private boolean gaveUp(int attempt) {
      if (!running.get()) {
        return true;
      }
      if (!resilience.reconnect()) {
        gaveUp = true;
        fireGiveUp();
        return true;
      }
      if (resilience.maxRetries() > 0 && attempt >= resilience.maxRetries()) {
        log.error(
            "Broadcaster reconnect exhausted after {} attempts; giving up so the worker can "
                + "restart",
            attempt);
        gaveUp = true;
        fireGiveUp();
        return true;
      }
      return false;
    }

    /**
     * R203: the wait before the next attempt, doubling to a ceiling, with half of it random.
     *
     * <p>Half fixed and half random: the fixed half keeps the wait growing, and the random half
     * keeps a fleet of replicas from arriving at a recovering backbone together.
     */
    double backoffSeconds(int attempt) {
      double base =
          attempt <= 0
              ? resilience.backoffMinSeconds()
              : resilience.backoffMinSeconds() * Math.pow(2, attempt - 1);
      base = Math.min(base, resilience.backoffMaxSeconds());
      return base / 2 + ThreadLocalRandom.current().nextDouble(0, Math.max(base / 2, 0.000001));
    }

    @Override
    public void publish(BroadcastNotification notification) {
      try {
        send(notification);
      } catch (Exception e) {
        bufferOutbound(notification, e);
      }
    }

    /**
     * R204: a publication the backbone would not take is kept, oldest dropped first.
     *
     * <p>It is not a delivery guarantee — the backbone keeps no replay of its own and a peer that
     * has not re-subscribed by the time the buffer is flushed still misses it. What it narrows is
     * the window in which two replicas disagree.
     */
    private void bufferOutbound(BroadcastNotification notification, Exception error) {
      synchronized (bufferLock) {
        if (resilience.replayBufferSize() <= 0) {
          log.warn("Broadcast to backbone failed ({}); replay buffer disabled", error.toString());
          return;
        }
        boolean overflow = outbound.size() >= resilience.replayBufferSize();
        if (overflow) {
          outbound.pollFirst();
        }
        outbound.addLast(notification);
        log.warn(
            "Broadcast to backbone failed ({}); buffered for replay ({}/{}{})",
            error.toString(),
            outbound.size(),
            resilience.replayBufferSize(),
            overflow ? ", OVERFLOW - oldest dropped" : "");
      }
    }

    /**
     * R208: one recovery at a time, and a gap that arrives during one is taken by that one.
     *
     * <p>Two concurrent recoveries would flush the same buffer twice and resync the fleet twice;
     * dropping the second would leave the gap that caused it unreconciled. Asking the running one
     * to go round again is neither.
     */
    private void scheduleGapRecovery() {
      synchronized (recoveryLock) {
        if (recoveryThread != null && recoveryThread.isAlive()) {
          log.debug("Gap recovery already in progress; requesting a rerun");
          recoveryRerunRequested = true;
          return;
        }
        recoveryRerunRequested = false;
        recoveryThread = new Thread(this::recoverAfterGap, "opal-broadcast-recovery");
        recoveryThread.setDaemon(true);
        recoveryThread.start();
      }
    }

    private void recoverAfterGap() {
      try {
        while (true) {
          synchronized (recoveryLock) {
            recoveryRerunRequested = false;
          }
          if (resilience.resyncSettleSeconds() > 0) {
            Thread.sleep((long) (resilience.resyncSettleSeconds() * 1000));
          }
          flushOutbound();
          fireReconnect();
          synchronized (recoveryLock) {
            if (!recoveryRerunRequested) {
              return;
            }
          }
          log.info("Gap arrived during recovery; rerunning flush and resync once");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (RuntimeException e) {
        log.error("Error during post-reconnect broadcast recovery", e);
      }
    }

    /**
     * Replays what was buffered, putting back what would not go.
     *
     * <p>The unsent tail goes back in front of anything buffered while the flush was running, so
     * the buffer stays in the order the publications were made.
     */
    private void flushOutbound() {
      List<BroadcastNotification> pending;
      synchronized (bufferLock) {
        if (outbound.isEmpty()) {
          return;
        }
        pending = new ArrayList<>(outbound);
        outbound.clear();
      }
      log.info("Replaying {} buffered broadcast(s) after recovery", pending.size());
      List<BroadcastNotification> unsent = new ArrayList<>(pending);
      while (!unsent.isEmpty()) {
        // R324: an entry that will not serialise is dropped rather than retried. It is not a
        // transport failure and no later flush will make it work, so leaving it at the head
        // wedges the buffer: every recovery after it replays nothing at all.
        try {
          write(unsent.get(0));
        } catch (RuntimeException e) {
          log.error(
              "Dropping un-serializable buffered broadcast on topic '{}': {}",
              unsent.get(0).topics(),
              e.toString());
          unsent.remove(0);
          continue;
        }
        try {
          send(unsent.get(0));
          unsent.remove(0);
        } catch (Exception e) {
          log.error(
              "Failed to replay buffered broadcasts ({} left, will retry on next recovery): {}",
              unsent.size(),
              e.toString());
          requeueUnsent(unsent);
          return;
        }
      }
    }

    private void requeueUnsent(List<BroadcastNotification> unsent) {
      synchronized (bufferLock) {
        List<BroadcastNotification> refill = new ArrayList<>(outbound);
        outbound.clear();
        outbound.addAll(unsent);
        outbound.addAll(refill);
        while (resilience.replayBufferSize() > 0 && outbound.size() > resilience.replayBufferSize()) {
          outbound.pollFirst();
        }
      }
    }

    private void fireReconnect() {
      if (onReconnect == null || !resilience.resyncOnReconnect()) {
        return;
      }
      try {
        onReconnect.run();
      } catch (RuntimeException e) {
        log.error("Broadcaster on_reconnect callback failed", e);
      }
    }

    /**
     * R337: the reader loop has ended and this worker is no longer reading the backbone.
     *
     * <p>Distinct from the give-up hook: giving up is one way to get here, and a reconnect the
     * configuration forbids is another, and both leave a worker whose fleet statistics can no
     * longer be relied on. Not fired while the process is closing down, where the loop ending is
     * what was asked for.
     */
    private void fireReaderEnded() {
      if (!running.get() || onReaderEnded == null) {
        return;
      }
      try {
        onReaderEnded.run();
      } catch (RuntimeException e) {
        log.error("Broadcaster on_reader_ended callback failed", e);
      }
    }

    /** R209: nothing else can bring a wedged reader back, so the process is told to end. */
    private void fireGiveUp() {
      if (onGiveUp == null) {
        log.error(
            "Broadcaster gave up reconnecting and no give-up hook is wired; this worker now "
                + "requires the liveness probe (/healthcheck) to be restarted");
        return;
      }
      try {
        onGiveUp.run();
      } catch (RuntimeException e) {
        log.error("Broadcaster on_give_up callback failed", e);
      }
    }

    @Override
    public final void close() {
      running.set(false);
      closeTransport();
      if (readerThread != null) {
        readerThread.interrupt();
        readerThread = null;
      }
      synchronized (recoveryLock) {
        if (recoveryThread != null) {
          recoveryThread.interrupt();
          recoveryThread = null;
        }
      }
    }

    /** How many publications are waiting to be replayed, which the tests read. */
    int bufferedCount() {
      synchronized (bufferLock) {
        return outbound.size();
      }
    }

    static String write(BroadcastNotification notification) {
      try {
        return Rpc.MAPPER.writeValueAsString(notification);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    static BroadcastNotification read(String payload) {
      try {
        return Rpc.MAPPER.readValue(payload, BroadcastNotification.class);
      } catch (Exception e) {
        log.warn("could not read a broadcast notification: {}", payload);
        return null;
      }
    }
  }

  /** Redis pub/sub, which is what OPAL's own default deployment uses. */
  static final class RedisBroadcaster extends Base {
    private final URI uri;
    private volatile Jedis subscriber;
    private volatile Jedis publisher;

    RedisBroadcaster(String uri, String channel) {
      super(channel);
      this.uri = URI.create(uri);
    }

    @Override
    void session(Reader reader, Runnable onSubscribed, Runnable onEvent) throws Exception {
      try (Jedis jedis = connect()) {
        subscriber = jedis;
        jedis.subscribe(
            new JedisPubSub() {
              @Override
              public void onSubscribe(String receivedChannel, int subscribedChannels) {
                onSubscribed.run();
              }

              @Override
              public void onMessage(String receivedChannel, String message) {
                onEvent.run();
                BroadcastNotification notification = read(message);
                if (notification != null) {
                  reader.onNotification(notification);
                }
              }
            },
            channel);
      } finally {
        subscriber = null;
      }
    }

    @Override
    void send(BroadcastNotification notification) {
      if (publisher == null) {
        publisher = connect();
      }
      publisher.publish(channel, write(notification));
    }

    private Jedis connect() {
      int port = uri.getPort() < 0 ? 6379 : uri.getPort();
      String host = uri.getHost() == null ? "localhost" : uri.getHost();
      Jedis jedis = new Jedis(host, port);
      String userInfo = uri.getUserInfo();
      if (userInfo != null && userInfo.contains(":")) {
        jedis.auth(userInfo.substring(userInfo.indexOf(':') + 1));
      }
      return jedis;
    }

    @Override
    void closeTransport() {
      Jedis open = subscriber;
      if (open != null) {
        try {
          open.close();
        } catch (Exception ignored) {
          // The reader thread is already unwinding; a close failure here has nothing left to fix.
        }
      }
      if (publisher != null) {
        publisher.close();
        publisher = null;
      }
    }
  }

  /** Postgres {@code LISTEN}/{@code NOTIFY}, which needs no extra infrastructure. */
  static final class PostgresBroadcaster extends Base {
    private final String jdbcUrl;
    private final Properties properties = new Properties();
    private volatile Connection listenConnection;
    private volatile Connection publishConnection;

    PostgresBroadcaster(String uri, String channel) {
      super(channel);
      URI parsed = URI.create(uri);
      String userInfo = parsed.getUserInfo();
      if (userInfo != null) {
        int colon = userInfo.indexOf(':');
        properties.setProperty("user", colon < 0 ? userInfo : userInfo.substring(0, colon));
        if (colon >= 0) {
          properties.setProperty("password", userInfo.substring(colon + 1));
        }
      }
      this.jdbcUrl =
          "jdbc:postgresql://"
              + (parsed.getHost() == null ? "localhost" : parsed.getHost())
              + (parsed.getPort() < 0 ? "" : ":" + parsed.getPort())
              + (parsed.getPath() == null ? "" : parsed.getPath());
    }

    @Override
    void session(Reader reader, Runnable onSubscribed, Runnable onEvent) throws Exception {
      Connection connection = DriverManager.getConnection(jdbcUrl, properties);
      listenConnection = connection;
      try (Statement statement = connection.createStatement()) {
        statement.execute("LISTEN \"" + channel + "\"");
      }
      onSubscribed.run();
      try {
        while (running.get() && !connection.isClosed()) {
          PGConnection pg = connection.unwrap(PGConnection.class);
          PGNotification[] notifications = pg.getNotifications(1000);
          if (notifications != null) {
            for (PGNotification notification : notifications) {
              onEvent.run();
              BroadcastNotification parsed = read(notification.getParameter());
              if (parsed != null) {
                reader.onNotification(parsed);
              }
            }
          }
        }
      } finally {
        listenConnection = null;
        closeQuietly(connection);
      }
    }

    @Override
    void send(BroadcastNotification notification) throws Exception {
      if (publishConnection == null || publishConnection.isClosed()) {
        publishConnection = DriverManager.getConnection(jdbcUrl, properties);
      }
      try (var statement = publishConnection.prepareStatement("SELECT pg_notify(?, ?)")) {
        statement.setString(1, channel);
        statement.setString(2, write(notification));
        statement.execute();
      }
    }

    @Override
    void closeTransport() {
      closeQuietly(listenConnection);
      closeQuietly(publishConnection);
      listenConnection = null;
      publishConnection = null;
    }

    private static void closeQuietly(Connection connection) {
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception ignored) {
          // Shutting down; a connection that will not close is already unusable.
        }
      }
    }
  }

  /** Kafka, where the channel is the topic name. */
  static final class KafkaBroadcaster extends Base {
    private final String servers;
    private volatile org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private volatile org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;

    KafkaBroadcaster(String uri, String channel) {
      super(channel);
      this.servers = URI.create(uri).getAuthority();
    }

    @Override
    void session(Reader reader, Runnable onSubscribed, Runnable onEvent) {
      Properties consumerProperties = new Properties();
      consumerProperties.put("bootstrap.servers", servers);
      consumerProperties.put("group.id", "opal-" + id());
      consumerProperties.put("auto.offset.reset", "latest");
      consumerProperties.put(
          "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      consumerProperties.put(
          "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      var open = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
          consumerProperties);
      consumer = open;
      try {
        open.subscribe(Collections.singletonList(channel));
        onSubscribed.run();
        while (running.get()) {
          var records = open.poll(Duration.ofMillis(500));
          records.forEach(
              record -> {
                onEvent.run();
                BroadcastNotification notification = read(record.value());
                if (notification != null) {
                  reader.onNotification(notification);
                }
              });
        }
      } finally {
        consumer = null;
        try {
          open.close(Duration.ofSeconds(1));
        } catch (Exception ignored) {
          // Already unwinding.
        }
      }
    }

    @Override
    void send(BroadcastNotification notification) {
      if (producer == null) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", servers);
        producerProperties.put(
            "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(
            "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties);
      }
      producer.send(
          new org.apache.kafka.clients.producer.ProducerRecord<>(
              channel, null, write(notification)));
    }

    @Override
    void closeTransport() {
      var open = consumer;
      if (open != null) {
        open.wakeup();
      }
      if (producer != null) {
        producer.close(Duration.ofSeconds(1));
        producer = null;
      }
    }
  }

  /** {@code memory://}, which relays inside one process — what a single-replica test uses. */
  static final class MemoryBroadcaster extends Base {
    private final Object idle = new Object();

    MemoryBroadcaster(String channel) {
      super(channel);
    }

    @Override
    void session(Reader received, Runnable onSubscribed, Runnable onEvent) throws Exception {
      onSubscribed.run();
      synchronized (idle) {
        while (running.get()) {
          idle.wait(200);
        }
      }
    }

    @Override
    void send(BroadcastNotification notification) {
      Reader current = reader;
      if (current != null) {
        current.onNotification(notification);
      }
    }

    @Override
    void closeTransport() {
      synchronized (idle) {
        idle.notifyAll();
      }
    }
  }
}
