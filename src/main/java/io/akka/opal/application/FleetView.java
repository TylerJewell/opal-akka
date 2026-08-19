package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.opal.domain.MemberEvent;
import java.util.List;

/**
 * Where every member of the fleet has got to. SPEC-001 rule 19.
 *
 * <p>Read by whoever wants to know whether the fleet is in step. A member's store is not here and
 * stays in the member itself: a fleet report is about positions. The fan-out does not read this,
 * because who belongs to a channel has to be ordered against the changes and this is built
 * afterwards.
 */
@Component(id = "fleet")
public class FleetView extends View {

  public record FleetMember(
      String id,
      String channel,
      long position,
      long applied,
      long duplicates,
      long gaps,
      boolean away) {}

  public record FleetMembers(List<FleetMember> members) {}

  @Consume.FromEventSourcedEntity(MemberEntity.class)
  public static class Updater extends TableUpdater<FleetMember> {

    public Effect<FleetMember> onEvent(MemberEvent event) {
      var id = updateContext().eventSubject().orElse("");
      var row =
          rowState() == null ? new FleetMember(id, "", 0, 0, 0, 0, false) : rowState();
      return effects()
          .updateRow(
              switch (event) {
                case MemberEvent.MemberJoined joined ->
                    new FleetMember(
                        id,
                        joined.channel(),
                        row.position(),
                        row.applied(),
                        row.duplicates(),
                        row.gaps(),
                        false);
                case MemberEvent.ChangeApplied applied ->
                    new FleetMember(
                        id,
                        row.channel(),
                        applied.change().position(),
                        row.applied() + 1,
                        row.duplicates(),
                        row.gaps(),
                        row.away());
                case MemberEvent.ChangeSkipped skipped ->
                    new FleetMember(
                        id,
                        row.channel(),
                        skipped.position(),
                        row.applied(),
                        row.duplicates(),
                        row.gaps(),
                        row.away());
                case MemberEvent.DuplicateSeen ignored ->
                    new FleetMember(
                        id,
                        row.channel(),
                        row.position(),
                        row.applied(),
                        row.duplicates() + 1,
                        row.gaps(),
                        row.away());
                case MemberEvent.GapRecorded ignored ->
                    new FleetMember(
                        id,
                        row.channel(),
                        row.position(),
                        row.applied(),
                        row.duplicates(),
                        row.gaps() + 1,
                        row.away());
                case MemberEvent.MemberLeft ignored ->
                    new FleetMember(
                        id,
                        row.channel(),
                        row.position(),
                        row.applied(),
                        row.duplicates(),
                        row.gaps(),
                        true);
                case MemberEvent.MemberReturned ignored ->
                    new FleetMember(
                        id,
                        row.channel(),
                        row.position(),
                        row.applied(),
                        row.duplicates(),
                        row.gaps(),
                        false);
              });
    }
  }

  @Query("SELECT * AS members FROM fleet")
  public QueryEffect<FleetMembers> everyMember() {
    return queryResult();
  }
}
