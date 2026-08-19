package io.akka.opal.domain;

/**
 * One instruction inside a change: put {@code value} at {@code destination}, for whoever is
 * watching {@code address}. SPEC-001 §2.2.
 *
 * <p>The value is carried inline. Fetching it from somewhere else is what the original also
 * supports and this port leaves out — see {@code docs/scope.md}.
 */
public record Entry(String address, String destination, String value) {

  public Entry {
    if (address == null) {
      throw new IllegalArgumentException("an entry needs an address");
    }
    if (destination == null) {
      throw new IllegalArgumentException("an entry needs a destination");
    }
  }

  public Address addressed() {
    return Address.of(address);
  }

  public Destination at() {
    return Destination.of(destination);
  }
}
