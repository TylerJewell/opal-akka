package io.akka.opal.common.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * The version of the package this process was built from — SPEC-002 R279.
 *
 * <p>Reported by {@code opal-server version}, by {@code GET /statistics} and by {@code GET
 * /stats}. The source asks its packaging metadata for the installed distribution's version; this
 * asks the build's, which is the same question of a different packaging system.
 *
 * <p>{@code 0.0.0} when there is no metadata to read — a run straight from compiled classes has
 * none, and so does a source checkout installed without one, which is why the original answers
 * {@code 0.0.0} there too.
 */
public final class Version {

  private static final String UNKNOWN = "0.0.0";

  private static final String RESOURCE = "/META-INF/maven/io.akka/opal-akka/pom.properties";

  private static final String VALUE = read();

  private Version() {}

  public static String current() {
    return VALUE;
  }

  private static String read() {
    try (InputStream in = Version.class.getResourceAsStream(RESOURCE)) {
      if (in == null) {
        String fromManifest = Version.class.getPackage().getImplementationVersion();
        return fromManifest == null || fromManifest.isBlank() ? UNKNOWN : fromManifest;
      }
      Properties properties = new Properties();
      properties.load(in);
      String version = properties.getProperty("version");
      return version == null || version.isBlank() ? UNKNOWN : version;
    } catch (Exception e) {
      return UNKNOWN;
    }
  }
}
