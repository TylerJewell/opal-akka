package io.akka.opal.common.rego;

import io.akka.opal.common.util.PurePath;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** What makes a file a policy module, a data module, or neither — SPEC-002 R24 to R26. */
public final class Rego {

  /**
   * A package declaration is a whole line: leading or trailing whitespace on it defeats the
   * match, and a file with no such line has an empty package name rather than a failure.
   */
  private static final Pattern PACKAGE_DECLARATION =
      Pattern.compile("^package\\s+([a-zA-Z0-9.\"\\[\\]]+)$");

  private Rego() {}

  /** R26: the first line that is a package declaration, or null. */
  public static String getRegoPackage(String contents) {
    if (contents == null) {
      return null;
    }
    for (String line : contents.split("\\R", -1)) {
      Matcher match = PACKAGE_DECLARATION.matcher(line);
      if (match.matches()) {
        return match.group(1);
      }
    }
    return null;
  }

  /** R24: only a file named exactly {@code data.json} is a data module. */
  public static boolean isDataModule(String path) {
    return PurePath.name(path).equals("data.json");
  }

  /** R24 and R25: a policy module's suffix is in the configured set, matched case-sensitively. */
  public static boolean isPolicyModule(String path, List<String> policyExtensions) {
    return policyExtensions.contains(PurePath.suffix(path));
  }
}
