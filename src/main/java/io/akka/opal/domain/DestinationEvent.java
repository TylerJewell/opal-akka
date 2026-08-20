package io.akka.opal.domain;

import akka.javasdk.annotations.TypeName;

/** What happens at a destination. Accepting a change is the only thing that does. */
public sealed interface DestinationEvent {

  @TypeName("change-accepted")
  record ChangeAccepted(Change change) implements DestinationEvent {}
}
