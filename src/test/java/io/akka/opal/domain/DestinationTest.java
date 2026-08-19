package io.akka.opal.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rule 14. A destination is a path of segments, so two destinations that merely share
 * a run of characters are different destinations. In the original they are compared as strings
 * (question-log row 7), which is why a change to {@code /user} holds up a change to
 * {@code /users}.
 */
class DestinationTest {

  @Test
  void twoDestinationsSharingAPrefixAreNotTheSame() {
    assertNotEquals(Destination.of("/user"), Destination.of("/users"));
  }

  @Test
  void aParentIsNotTheSameAsItsChild() {
    assertNotEquals(Destination.of("/a"), Destination.of("/a/b"));
  }

  @Test
  void aParentContainsItsChild() {
    assertEquals(true, Destination.of("/a").contains(Destination.of("/a/b")));
    assertEquals(false, Destination.of("/user").contains(Destination.of("/users")));
    assertEquals(false, Destination.of("/a/b").contains(Destination.of("/a")));
  }

  @Test
  void aDestinationContainsItself() {
    assertEquals(true, Destination.of("/a/b").contains(Destination.of("/a/b")));
  }

  @Test
  void theSameDestinationWrittenTwoWaysIsOneDestination() {
    assertEquals(Destination.of("/a/b"), Destination.of("a/b"));
    assertEquals(Destination.of("/a/b"), Destination.of("/a/b/"));
    assertEquals(Destination.of("/a//b"), Destination.of("/a/b"));
  }

  @Test
  void theRootIsADestination() {
    assertEquals(Destination.root(), Destination.of("/"));
    assertEquals(Destination.root(), Destination.of(""));
    assertEquals(true, Destination.root().contains(Destination.of("/anything/at/all")));
  }

  @Test
  void aDestinationReadsBackAsItWasNormalised() {
    assertEquals("/a/b", Destination.of("a//b/").path());
    assertEquals("/", Destination.root().path());
  }

  @Test
  void refusesADestinationThatIsNotThere() {
    assertThrows(IllegalArgumentException.class, () -> Destination.of(null));
  }
}
