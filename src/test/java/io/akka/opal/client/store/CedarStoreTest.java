package io.akka.opal.client.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R86 — the second policy engine, and where it differs from the first.
 *
 * <p>Cedar and OPA answer the same route shapes, so the interesting facts are the two places they
 * do not line up: Cedar holds no static data cache, which is why offline mode refuses it, and it
 * addresses its data as one whole document rather than as a tree of paths.
 */
class CedarStoreTest {

  private static CedarClient client(String url) {
    return new CedarClient(url, null, PolicyStoreAuth.NONE, List.of(),
        new ConnRetryOptions(null, 0.0, 2, 0.0));
  }

  /** The method set the source's own client exposes, matched against this one's. */
  @Test
  void theStoreInterfaceCoversEveryMethodTheSourceHas() {
    JsonNode recorded = SourceAnswers.get("cedar_client_shape");

    // The two the source spells differently: `end_transcation` is its own typo, and the two
    // `start`/`stop_liveness_probe` methods are one object on this side.
    List<String> exceptions =
        List.of("end_transcation", "start_transaction", "start_liveness_probe",
            "stop_liveness_probe", "transaction_context", "set_policies");

    List<String> here = new ArrayList<>();
    for (java.lang.reflect.Method method : PolicyStoreClient.class.getMethods()) {
      here.add(snake(method.getName()));
    }

    List<String> missing = new ArrayList<>();
    for (String name : SourceAnswers.strings(recorded.get("methods"))) {
      if (!exceptions.contains(name) && !here.contains(name)) {
        missing.add(name);
      }
    }
    assertEquals(List.of(), missing, "methods the source's store has and this one does not");
    assertTrue(here.contains("set_policies"), "the bundle write is on the interface");
  }

  /** R86: no static cache, which is the reason offline mode refuses a Cedar store. */
  @Test
  void thereIsNoStaticDataCache() {
    JsonNode recorded = SourceAnswers.get("cedar_client_shape");
    assertFalse(recorded.get("has_static_cache").asBoolean(), "the source has none either");

    boolean found = false;
    for (java.lang.reflect.Field field : CedarClient.class.getDeclaredFields()) {
      found = found || field.getType() == OpaStaticDataCache.class;
    }
    assertFalse(found, "CedarClient holds no OpaStaticDataCache");
  }

  /** Cedar addresses its data as one document, so a named path is refused rather than translated. */
  @Test
  void aNamedDataPathIsRefused() {
    CedarClient cedar = client("http://127.0.0.1:1");
    assertThrows(
        IllegalArgumentException.class,
        () -> cedar.setPolicyData(SourceAnswers.MAPPER.createArrayNode(), "/users", null));
    assertThrows(
        IllegalArgumentException.class, () -> cedar.deletePolicyData("/users", null));
    assertThrows(
        IllegalArgumentException.class, () -> cedar.patchPolicyData(List.of(), "", null));
  }

  private static String snake(String name) {
    StringBuilder out = new StringBuilder();
    for (char c : name.toCharArray()) {
      if (Character.isUpperCase(c)) {
        out.append('_').append(Character.toLowerCase(c));
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }
}
