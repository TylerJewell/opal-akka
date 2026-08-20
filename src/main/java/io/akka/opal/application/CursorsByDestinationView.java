package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.opal.domain.CursorEvent;
import java.util.List;

/**
 * Whether the fleet has caught up — SPEC-001 §3 rule 19, §4 OD-4.
 *
 * <p>The source cannot answer this: a report names one subscriber and a change identity that
 * subscriber invented for itself (question-log row 23), so there is nothing to join on. Here
 * every cursor is a row, and "who is behind on this destination" is a query.
 *
 * <p>The row carries the position, not the distance from the front. How far behind a
 * subscriber is depends on where the destination has got to, which this view does not see —
 * the caller subtracts.
 */
@Component(id = "cursors-by-destination")
public class CursorsByDestinationView extends View {

  /**
   * @param applied the highest change applied with nothing missing before it
   * @param seen the highest change this subscriber has been handed, applied or not. A gap is
   *     open exactly when it is ahead of {@code applied} — the cursor's own invariant, since
   *     a change above the next expected one is never applied.
   */
  public record CursorEntry(
      String id, String subscriber, String destination, long applied, long seen) {

    public boolean hasGap() {
      return seen > applied;
    }
  }

  public record Cursors(List<CursorEntry> items) {}

  public record BehindQuery(String destination, long sequence) {}

  @Consume.FromEventSourcedEntity(CursorEntity.class)
  public static class CursorsUpdater extends TableUpdater<CursorEntry> {

    public Effect<CursorEntry> onEvent(CursorEvent event) {
      String id = updateContext().eventSubject().get();
      CursorEntry current = rowState();
      long applied = current == null ? 0 : current.applied();
      long seen = current == null ? 0 : current.seen();

      return switch (event) {
        case CursorEvent.ChangeApplied e -> effects()
            .updateRow(row(id, e.sequence(), Math.max(seen, e.sequence())));
        case CursorEvent.DuplicateIgnored ignored -> effects().updateRow(row(id, applied, seen));
        case CursorEvent.GapRecorded e -> effects()
            .updateRow(row(id, applied, Math.max(seen, e.to())));
        case CursorEvent.ValueTaken e -> effects().updateRow(row(id, e.sequence(), e.sequence()));
      };
    }

    private CursorEntry row(String id, long applied, long seen) {
      CursorEntity.Id parsed = CursorEntity.Id.parse(id);
      return new CursorEntry(id, parsed.subscriber(), parsed.destination(), applied, seen);
    }
  }

  @Query("SELECT * AS items FROM cursors_by_destination WHERE destination = :destination "
      + "ORDER BY subscriber")
  public QueryEffect<Cursors> byDestination(String destination) {
    return queryResult();
  }

  @Query("SELECT * AS items FROM cursors_by_destination WHERE destination = :destination "
      + "AND applied < :sequence ORDER BY subscriber")
  public QueryEffect<Cursors> behind(BehindQuery query) {
    return queryResult();
  }
}
