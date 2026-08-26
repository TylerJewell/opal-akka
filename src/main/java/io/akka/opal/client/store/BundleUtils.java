package io.akka.opal.client.store;

import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.util.PurePath;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The order modules are loaded in — SPEC-002 R80.
 *
 * <p>A module the manifest does not name sorts to 10000, and the sort is stable, so two unnamed
 * modules keep the order the bundle put them in. That matters because a data module's position
 * decides which of two writes to overlapping paths lands last.
 */
public final class BundleUtils {

  public static final int MAX_INDEX = 10000;

  private BundleUtils() {}

  public static List<Policy.RegoModule> sortedPolicyModulesToLoad(Policy.PolicyBundle bundle) {
    List<String> manifest = normalized(bundle.manifest());
    List<Policy.RegoModule> modules = new ArrayList<>(bundle.policy_modules());
    modules.sort(Comparator.comparingInt(module -> indexOf(manifest, module.path())));
    return modules;
  }

  public static List<Policy.DataModule> sortedDataModulesToLoad(Policy.PolicyBundle bundle) {
    List<String> manifest = normalized(bundle.manifest());
    List<Policy.DataModule> modules = new ArrayList<>(bundle.data_modules());
    modules.sort(Comparator.comparingInt(module -> indexOf(manifest, module.path())));
    return modules;
  }

  /** Already ordered by the bundle maker, and reversed there — R32. */
  public static List<String> sortedPolicyModulesToDelete(Policy.PolicyBundle bundle) {
    return bundle.deleted_files() == null ? List.of() : bundle.deleted_files().policy_modules();
  }

  public static List<String> sortedDataModulesToDelete(Policy.PolicyBundle bundle) {
    return bundle.deleted_files() == null ? List.of() : bundle.deleted_files().data_modules();
  }

  private static List<String> normalized(List<String> manifest) {
    List<String> out = new ArrayList<>(manifest.size());
    for (String path : manifest) {
      out.add(PurePath.normalize(path));
    }
    return out;
  }

  private static int indexOf(List<String> manifest, String path) {
    int index = manifest.indexOf(PurePath.normalize(path));
    return index < 0 ? MAX_INDEX : index;
  }
}
