package io.akka.opal.domain;

import java.util.List;

/**
 * What one member of the fleet has asked to hear about — SPEC-001 §2.
 *
 * <p>The topics are as the subscriber wrote them, unexpanded. Expansion happens on the
 * publishing side, so a subscriber to a whole branch is matched by string equality against a
 * change addressed to a leaf under it (§3 rule 3).
 */
public record Subscription(String subscriber, List<String> topics) {}
