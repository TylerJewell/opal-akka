package io.akka.opal.common.git;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * A reader for the USTAR/GNU tar format, enough of it to unpack an OPA bundle.
 *
 * <p>Written here rather than taken from a library because the only thing needed is a sequential
 * walk over the members, and the extraction that follows has its own rules about which members it
 * will accept.
 */
public final class Tar {

  /** Called once per member, with a stream positioned at that member's content. */
  public interface Visitor {
    void visit(String name, boolean isDirectory, long size, InputStream content) throws IOException;
  }

  private static final int BLOCK = 512;

  private Tar() {}

  public static void forEach(InputStream in, Visitor visitor) throws IOException {
    byte[] header = new byte[BLOCK];
    String longName = null;
    while (true) {
      if (!readFully(in, header)) {
        return;
      }
      if (isAllZero(header)) {
        return;
      }
      String name = text(header, 0, 100);
      String prefix = text(header, 345, 155);
      if (!prefix.isEmpty()) {
        name = prefix + "/" + name;
      }
      long size = octal(header, 124, 12);
      char typeFlag = (char) header[156];

      if (typeFlag == 'L') {
        byte[] body = new byte[(int) size];
        readFully(in, body);
        skipPadding(in, size);
        longName = new String(body, StandardCharsets.UTF_8).trim();
        continue;
      }
      if (longName != null) {
        name = longName;
        longName = null;
      }
      // 'x' and 'g' are pax headers, which carry metadata rather than a file.
      if (typeFlag == 'x' || typeFlag == 'g') {
        skip(in, size);
        skipPadding(in, size);
        continue;
      }

      // R191: a member that is a link or a device is refused rather than written.
      //
      // A symbolic or hard link naming a path outside the extraction directory would be a way
      // to write anywhere the process can reach, and a character or block device is not a thing
      // a policy bundle contains. Both are refused whatever they point at: nothing in a bundle
      // needs either, so there is no case to distinguish.
      if (typeFlag == '1' || typeFlag == '2') {
        String linkName = text(header, 157, 100);
        // The two are refused for the same reason and named differently, because the source
        // names them differently and a caller reading the message is told which kind it was.
        throw new IOException(
            "Attempted directory traversal via "
                + (typeFlag == '2' ? "symlink" : "link")
                + " for member: "
                + linkName);
      }
      if (typeFlag == '3' || typeFlag == '4') {
        throw new IOException("tarfile returns true for isblk() or ischr()");
      }

      boolean isDirectory = typeFlag == '5' || name.endsWith("/");
      String cleaned = name.endsWith("/") ? name.substring(0, name.length() - 1) : name;
      if (isDirectory) {
        visitor.visit(cleaned, true, 0, InputStream.nullInputStream());
      } else {
        byte[] body = new byte[(int) size];
        readFully(in, body);
        visitor.visit(cleaned, false, size, new ByteArrayInputStream(body));
      }
      if (!isDirectory) {
        skipPadding(in, size);
      } else {
        skip(in, size);
        skipPadding(in, size);
      }
    }
  }

  private static boolean isAllZero(byte[] block) {
    for (byte b : block) {
      if (b != 0) {
        return false;
      }
    }
    return true;
  }

  private static String text(byte[] block, int offset, int length) {
    int end = offset;
    while (end < offset + length && block[end] != 0) {
      end++;
    }
    return new String(block, offset, end - offset, StandardCharsets.UTF_8);
  }

  private static long octal(byte[] block, int offset, int length) {
    String value = text(block, offset, length).trim();
    if (value.isEmpty()) {
      return 0;
    }
    try {
      return Long.parseLong(value, 8);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static boolean readFully(InputStream in, byte[] buffer) throws IOException {
    int read = 0;
    while (read < buffer.length) {
      int count = in.read(buffer, read, buffer.length - read);
      if (count < 0) {
        return read > 0 && read == buffer.length;
      }
      read += count;
    }
    return true;
  }

  private static void skip(InputStream in, long count) throws IOException {
    long remaining = count;
    byte[] scratch = new byte[8192];
    while (remaining > 0) {
      int read = in.read(scratch, 0, (int) Math.min(scratch.length, remaining));
      if (read < 0) {
        return;
      }
      remaining -= read;
    }
  }

  private static void skipPadding(InputStream in, long size) throws IOException {
    long remainder = size % BLOCK;
    if (remainder != 0) {
      skip(in, BLOCK - remainder);
    }
  }
}
