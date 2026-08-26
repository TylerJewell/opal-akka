package io.akka.opal.common.monitoring;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Just enough MessagePack to write a v0.5 trace payload — SPEC-002 R162.
 *
 * <p>The agent's v0.5 endpoint reads one array of two elements: a table of every string in the
 * payload, and the traces, whose spans refer to that table by index. Only the types that payload
 * uses are written here — arrays, maps, strings, signed and unsigned 64-bit integers, and
 * doubles.
 */
final class MsgPack {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream(4096);

  byte[] bytes() {
    return out.toByteArray();
  }

  MsgPack array(int size) {
    if (size < 16) {
      out.write(0x90 | size);
    } else if (size < 0x10000) {
      out.write(0xdc);
      writeShort(size);
    } else {
      out.write(0xdd);
      writeInt(size);
    }
    return this;
  }

  MsgPack map(int size) {
    if (size < 16) {
      out.write(0x80 | size);
    } else if (size < 0x10000) {
      out.write(0xde);
      writeShort(size);
    } else {
      out.write(0xdf);
      writeInt(size);
    }
    return this;
  }

  MsgPack string(String value) {
    byte[] raw = value.getBytes(StandardCharsets.UTF_8);
    if (raw.length < 32) {
      out.write(0xa0 | raw.length);
    } else if (raw.length < 0x100) {
      out.write(0xd9);
      out.write(raw.length);
    } else if (raw.length < 0x10000) {
      out.write(0xda);
      writeShort(raw.length);
    } else {
      out.write(0xdb);
      writeInt(raw.length);
    }
    out.writeBytes(raw);
    return this;
  }

  /** A whole number, written in the narrowest form that holds it. */
  MsgPack integer(long value) {
    if (value >= 0 && value < 128) {
      out.write((int) value);
    } else if (value < 0 && value >= -32) {
      out.write((int) (value & 0xff));
    } else {
      out.write(0xcf);
      for (int shift = 56; shift >= 0; shift -= 8) {
        out.write((int) ((value >>> shift) & 0xff));
      }
    }
    return this;
  }

  MsgPack real(double value) {
    out.write(0xcb);
    long bits = Double.doubleToRawLongBits(value);
    for (int shift = 56; shift >= 0; shift -= 8) {
      out.write((int) ((bits >>> shift) & 0xff));
    }
    return this;
  }

  private void writeShort(int value) {
    out.write((value >>> 8) & 0xff);
    out.write(value & 0xff);
  }

  private void writeInt(int value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
      out.write((value >>> shift) & 0xff);
    }
  }
}
