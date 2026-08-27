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

    /**
     * A link member whose target stays inside the extraction directory.
     *
     * <p>Ignored unless the caller wants it: the walk's job is to refuse the members that are
     * unsafe, and a caller that has no use for a link is not made to handle one.
     */
    default void visitLink(String name, String target, boolean symbolic) throws IOException {}
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

      // R191: a link member is refused when its target leaves the extraction directory, and a
      // device member is refused whatever it is.
      //
      // A link naming a path outside the directory is a way to write anywhere the process can
      // reach; one naming a path inside it is an ordinary part of an archive. The two kinds are
      // named differently in the refusal because the source names them differently and a caller
      // reading the message is told which kind it was.
      if (typeFlag == '1' || typeFlag == '2') {
        String linkName = text(header, 157, 100);
        if (escapes(linkName)) {
          throw new IOException(
              "Attempted directory traversal via "
                  + (typeFlag == '2' ? "symlink" : "link")
                  + " for member: "
                  + linkName);
        }
        visitor.visitLink(name, linkName, typeFlag == '2');
        skip(in, size);
        skipPadding(in, size);
        continue;
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

  /**
   * Whether a member's own name or a link's target reaches outside the directory it is being
   * written into. An absolute path is outside by definition; a relative one is outside when its
   * {@code ..} segments outnumber the segments they could climb.
   */
  static boolean escapes(String path) {
    if (path == null || path.isEmpty()) {
      return false;
    }
    if (path.startsWith("/") || path.matches("^[A-Za-z]:[\\/].*")) {
      return true;
    }
    int depth = 0;
    for (String part : path.split("/")) {
      if (part.isEmpty() || part.equals(".")) {
        continue;
      }
      if (part.equals("..")) {
        depth--;
        if (depth < 0) {
          return true;
        }
      } else {
        depth++;
      }
    }
    return false;
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
