package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.timedaction.TimedAction;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.CursorState;
import io.akka.opal.domain.Outcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closing a gap — SPEC-001 §3 rules 16 to 18, §4 OD-4.
 *
 * <p>Asks the destination for exactly what this subscriber is missing and applies it in
 * order. Where the gap reaches further back than the destination's retained log, the
 * subscriber takes the current value outright instead — the source's only recovery, kept
 * here as one option rather than the only one.
 *
 * <p>Safe to run when nothing is missing: the span comes back empty and nothing is applied.
 */
@Component(id = "catch-up")
public class CatchUpAction extends TimedAction {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpAction.class);

  public record Request(String subscriber, String destination) {}

  private final ComponentClient componentClient;

  public CatchUpAction(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public Effect catchUp(Request request) {
    String cursorId = CursorEntity.Id.of(request.subscriber(), request.destination());
    CursorState cursor =
        componentClient.forEventSourcedEntity(cursorId).method(CursorEntity::read).invoke();

    DestinationEntity.Span span =
        componentClient
            .forEventSourcedEntity(request.destination())
            .method(DestinationEntity::since)
            .invoke(cursor.applied());

    if (!span.complete()) {
      logger.info(
          "{} is further behind {} than the retained log reaches; taking the current value",
          request.subscriber(),
          request.destination());
      span.value()
          .ifPresent(
              value ->
                  componentClient
                      .forEventSourcedEntity(cursorId)
                      .method(CursorEntity::takeValue)
                      .invoke(new CursorEntity.TakeValue(span.sequence(), value)));
      return effects().done();
    }

    for (Change change : span.changes()) {
      CursorEntity.Result result =
          componentClient
              .forEventSourcedEntity(cursorId)
              .method(CursorEntity::apply)
              .invoke(new CursorEntity.Apply(change.sequence(), change.payload()));
      if (result.outcome() == Outcome.GAP) {
        // Reading the span and applying it are two steps, so a change delivered in between
        // can move the cursor on. Stopping here leaves the rest for the next catch-up
        // rather than replaying a span that no longer starts where this one does.
        logger.info(
            "catch-up for {} on {} stopped at {}: the span no longer starts at the cursor",
            request.subscriber(),
            request.destination(),
            change.sequence());
        break;
      }
    }
    return effects().done();
  }
}
