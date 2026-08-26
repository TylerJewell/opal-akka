package io.akka.opal.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/** The policy half of OPAL's wire schema — SPEC-002 section 2.1. */
public final class Policy {

  private Policy() {}

  public record DataModule(String path, String data) {}

  public record RegoModule(String path, String package_name, String rego) {}

  public record DeletedFiles(List<String> data_modules, List<String> policy_modules) {
    public static DeletedFiles empty() {
      return new DeletedFiles(List.of(), List.of());
    }

    @JsonIgnore
    public boolean isEmpty() {
      return data_modules.isEmpty() && policy_modules.isEmpty();
    }
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record PolicyBundle(
      List<String> manifest,
      String hash,
      String old_hash,
      List<DataModule> data_modules,
      List<RegoModule> policy_modules,
      DeletedFiles deleted_files) {

    @JsonIgnore
    public boolean isDiff() {
      return old_hash != null;
    }
  }

  public record PolicyUpdateMessage(
      String old_policy_hash, String new_policy_hash, List<String> changed_directories) {}

  public record PolicyUpdateMessageNotification(PolicyUpdateMessage update, List<String> topics) {}
}
