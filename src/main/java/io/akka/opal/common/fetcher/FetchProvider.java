package io.akka.opal.common.fetcher;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Something that knows how to get one thing from one place — SPEC-002 R146.
 *
 * <p>Fetching and processing are separate because a provider may want to hand back what it got
 * without reading it: a caller that has asked for the response itself, rather than for the value
 * inside it, gets the response.
 */
public interface FetchProvider extends AutoCloseable {

  /** Retrieves the raw result, retrying on the provider's own terms. */
  Object fetch();

  /** Turns the raw result into what the caller asked for. */
  JsonNode process(Object raw);

  /** Opens whatever the provider needs for the length of one fetch. */
  default FetchProvider open() {
    return this;
  }

  @Override
  default void close() {}
}
