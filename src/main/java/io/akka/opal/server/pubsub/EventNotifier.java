package io.akka.opal.server.pubsub;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The pub/sub broker — SPEC-002 R60 to R63.
 *
 * <p>A subscription is one topic for one subscriber, so a subscriber matching three of a
 * publication's topics is notified three times, each naming the topic that matched. That is not
 * a redundancy: the receiver dispatches on the topic it was told about, and coalescing the
 * three into one would leave it unable to tell which of its interests fired.
 */
public final class EventNotifier {

  private static final Logger log = LoggerFactory.getLogger(EventNotifier.class);

  /** What a subscriber is told about, with the callback removed before it goes on the wire. */
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record Subscription(String id, String subscriber_id, String topic, String notifier_id) {}

  /** Raised by a restriction to refuse a subscribe or a publish. */
  public static final class Refused extends RuntimeException {
    public Refused(String message) {
      super(message);
    }
  }

  /** Runs on every subscribe and publish that arrived on a channel. */
  public interface ChannelRestriction {
    void check(List<String> topics, RpcChannel channel);
  }

  private final Map<String, Map<String, List<Held>>> topics = new ConcurrentHashMap<>();
  private final List<ChannelRestriction> restrictions = new ArrayList<>();
  private final List<BiConsumer<String, List<String>>> onSubscribe = new ArrayList<>();
  private final List<BiConsumer<String, List<String>>> onUnsubscribe = new ArrayList<>();

  private record Held(Subscription subscription, Callback callback) {}

  /** Delivers one notification to one subscriber. */
  public interface Callback {
    void notify(Subscription subscription, Object data);
  }

  public void addChannelRestriction(ChannelRestriction restriction) {
    restrictions.add(restriction);
  }

  public void registerSubscribeEvent(BiConsumer<String, List<String>> callback) {
    onSubscribe.add(callback);
  }

  public void registerUnsubscribeEvent(BiConsumer<String, List<String>> callback) {
    onUnsubscribe.add(callback);
  }

  public static String generateId() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public synchronized List<Subscription> subscribe(
      String subscriberId, List<String> topicList, Callback callback, RpcChannel channel) {
    if (channel != null) {
      for (ChannelRestriction restriction : restrictions) {
        restriction.check(topicList, channel);
      }
    }
    List<Subscription> created = new ArrayList<>();
    for (String topic : topicList) {
      Subscription subscription =
          new Subscription(generateId(), subscriberId, topic, null);
      topics
          .computeIfAbsent(topic, ignored -> new LinkedHashMap<>())
          .computeIfAbsent(subscriberId, ignored -> new ArrayList<>())
          .add(new Held(subscription, callback));
      created.add(subscription);
      log.debug("New subscription {}", subscription);
    }
    onSubscribe.forEach(handler -> handler.accept(subscriberId, topicList));
    return created;
  }

  /** A null topic list unsubscribes the subscriber from everything it holds. */
  public synchronized void unsubscribe(String subscriberId, List<String> topicList) {
    List<String> effective = topicList == null ? new ArrayList<>(topics.keySet()) : topicList;
    for (String topic : effective) {
      Map<String, List<Held>> subscribers = topics.get(topic);
      if (subscribers != null) {
        subscribers.remove(subscriberId);
      }
    }
    onUnsubscribe.forEach(handler -> handler.accept(subscriberId, effective));
  }

  /**
   * R60 and R62. The notifier's own id is never notified, which is what stops a publisher from
   * receiving its own publication back through the same channel; a subscriber of
   * {@code ALL_TOPICS} is told the topic that actually fired rather than the sentinel.
   */
  public void notify(List<String> topicList, Object data, String notifierId, RpcChannel channel) {
    if (channel != null) {
      for (ChannelRestriction restriction : restrictions) {
        restriction.check(topicList, channel);
      }
    }
    List<Runnable> deliveries = new ArrayList<>();
    synchronized (this) {
      Map<String, List<Held>> subscribersToAll = topics.getOrDefault(Rpc.ALL_TOPICS, Map.of());
      for (String topic : topicList) {
        Map<String, List<Held>> subscribers = topics.getOrDefault(topic, Map.of());
        collect(deliveries, copyOf(subscribers), topic, data, notifierId, false);
        collect(deliveries, copyOf(subscribersToAll), topic, data, notifierId, true);
      }
    }
    for (Runnable delivery : deliveries) {
      delivery.run();
    }
  }

  private static Map<String, List<Held>> copyOf(Map<String, List<Held>> subscribers) {
    Map<String, List<Held>> copy = new LinkedHashMap<>();
    subscribers.forEach((id, held) -> copy.put(id, new ArrayList<>(held)));
    return copy;
  }

  private void collect(
      List<Runnable> deliveries,
      Map<String, List<Held>> subscribers,
      String topic,
      Object data,
      String notifierId,
      boolean overrideTopic) {
    subscribers.forEach(
        (subscriberId, held) -> {
          if (subscriberId.equals(notifierId)) {
            return;
          }
          for (Held one : held) {
            Subscription subscription =
                overrideTopic
                    ? new Subscription(
                        one.subscription().id(),
                        one.subscription().subscriber_id(),
                        topic,
                        notifierId)
                    : new Subscription(
                        one.subscription().id(),
                        one.subscription().subscriber_id(),
                        one.subscription().topic(),
                        notifierId);
            deliveries.add(
                () -> {
                  try {
                    one.callback().notify(subscription, data);
                  } catch (Exception e) {
                    log.warn(
                        "Failed to notify subscriber sub_id={} with topic={}", subscriberId, topic,
                        e);
                  }
                });
          }
        });
  }

  /** The topics anybody currently holds a subscription on. */
  public synchronized List<String> knownTopics() {
    return new ArrayList<>(topics.keySet());
  }

  public synchronized boolean hasTopic(String topic) {
    return topics.containsKey(topic);
  }
}
