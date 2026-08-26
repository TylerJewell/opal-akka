package io.akka.opal.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.common.confi.Confi;
import io.akka.opal.server.config.ServerConfig;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R6 and R10, checked against the source rather than against a transcription: the three
 * files under {@code src/test/resources/source} are what the original's own {@code print-config}
 * printed, and this compares the rebuild's entry set and every printed value against them.
 *
 * <p>The environment is empty rather than inherited, because a real {@code OPAL_} variable in
 * the shell running the tests would move a value out from under the baseline.
 */
class ConfigCensusTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static Map<String, String> baseline(String name) throws Exception {
    try (InputStream in =
        ConfigCensusTest.class.getResourceAsStream("/source/printconfig-" + name + ".json")) {
      return MAPPER.readValue(in, MAPPER.getTypeFactory().constructMapType(
          TreeMap.class, String.class, String.class));
    }
  }

  private static Map<String, String> printed(Confi config) throws Exception {
    return MAPPER.readValue(config.printConfig(), MAPPER.getTypeFactory().constructMapType(
        TreeMap.class, String.class, String.class));
  }

  /** Values that name a directory of the machine the baseline was taken on. */
  private static final List<String> MACHINE_SPECIFIC =
      List.of("AUTH_JWKS_STATIC_DIR", "POLICY_REPO_CLONE_PATH", "BASE_DIR", "GIT_SSH_KEY_FILE");

  private void compare(String name, Confi config) throws Exception {
    Map<String, String> expected = baseline(name);
    Map<String, String> actual = printed(config);

    assertEquals(
        new TreeSet<>(expected.keySet()),
        new TreeSet<>(actual.keySet()),
        name + ": the entry set is the source's own");

    List<String> differences = new ArrayList<>();
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      if (MACHINE_SPECIFIC.contains(entry.getKey())) {
        continue;
      }
      String mine = actual.get(entry.getKey());
      if (!entry.getValue().equals(mine)) {
        differences.add(entry.getKey() + ":\n  source " + entry.getValue() + "\n  port   " + mine);
      }
    }
    assertTrue(differences.isEmpty(), name + " printed differently:\n" + String.join("\n", differences));
  }

  @Test
  void commonEntriesMatchTheSource() throws Exception {
    compare("common", new CommonConfig(Map.of()));
  }

  @Test
  void serverEntriesMatchTheSource() throws Exception {
    compare("server", new ServerConfig(Map.of()));
  }

  @Test
  void clientEntriesMatchTheSource() throws Exception {
    compare("client", new ClientConfig(Map.of()));
  }

  @Test
  void theCountsAreFortyEightyTwoAndFiftyFive() {
    assertEquals(40, new CommonConfig(Map.of()).entries().size());
    assertEquals(82, new ServerConfig(Map.of()).entries().size());
    assertEquals(55, new ClientConfig(Map.of()).entries().size());
  }

  @Test
  void aMachineSpecificDefaultStillPointsSomewhereNamed() {
    ServerConfig server = new ServerConfig(Map.of());
    assertTrue(server.getString("AUTH_JWKS_STATIC_DIR").endsWith("jwks_dir"));
    assertTrue(server.getString("POLICY_REPO_CLONE_PATH").endsWith("regoclone"));
    assertTrue(server.getString("BASE_DIR").endsWith(".local/state/opal"));
    assertTrue(
        new CommonConfig(Map.of()).getString("GIT_SSH_KEY_FILE").endsWith("opal_repo_ssh_key"));
  }
}
