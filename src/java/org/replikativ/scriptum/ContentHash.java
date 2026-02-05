package org.replikativ.scriptum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Content-addressable hashing for Lucene segments using SHA-512.
 *
 * <p>This class provides cryptographic hashing utilities compatible with the hasch library used by
 * Datahike, Proximum, and Yggdrasil. It enables merkle tree commits where the commit UUID is
 * derived from the hash of its content (parent hash + segment hashes).
 *
 * <p>Algorithm: SHA-512 hash → UUID5 (matches hasch.platform/uuid5)
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Hash a segment's files
 * Map<String, Path> files = Map.of("_0.cfs", path1, "_0.si", path2);
 * Map<String, UUID> hashes = ContentHash.hashSegmentFiles(files);
 *
 * // Compute commit hash
 * UUID commitHash = ContentHash.computeCommitHash(parentUuid, segmentHashes);
 * }</pre>
 *
 * @see <a href="https://github.com/replikativ/hasch">hasch library</a>
 */
public class ContentHash {

  /**
   * Hash a byte array to UUID5 using SHA-512.
   *
   * <p>This method matches the hasch.platform/uuid5 implementation: - Computes SHA-512 hash of
   * input - Takes first 16 bytes of hash - Sets UUID version to 5 and variant to RFC4122
   *
   * @param data Bytes to hash
   * @return UUID5 derived from SHA-512 hash
   * @throws RuntimeException if SHA-512 algorithm is not available
   */
  public static UUID hashBytes(byte[] data) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-512");
      byte[] hash = md.digest(data);
      return bytesToUUID5(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-512 not available", e);
    }
  }

  /**
   * Hash a file's contents to UUID5.
   *
   * <p>Reads the entire file into memory and computes its SHA-512 hash. For very large files (>1GB
   * segments), consider streaming approaches.
   *
   * @param filePath Path to file to hash
   * @return UUID5 of file contents
   * @throws IOException if file cannot be read
   */
  public static UUID hashFile(Path filePath) throws IOException {
    byte[] fileBytes = Files.readAllBytes(filePath);
    return hashBytes(fileBytes);
  }

  /**
   * Convert SHA-512 hash to UUID5.
   *
   * <p>Matches hasch.platform/uuid5 implementation exactly: - Takes first 16 bytes of hash as UUID
   * high/low longs - Sets version to 5 (bits 12-15 of high long) - Sets variant to RFC4122 (bits
   * 62-63 of low long)
   *
   * @param hash SHA-512 hash (64 bytes)
   * @return UUID5 with correct version and variant bits
   */
  private static UUID bytesToUUID5(byte[] hash) {
    ByteBuffer bb = ByteBuffer.wrap(hash);
    long high = bb.getLong();
    long low = bb.getLong();

    // UUID5 version and variant bits - matches hasch.platform/uuid5 exactly
    // Step by step translation from Clojure:
    // (-> high (bit-or 0x5000) (bit-and 0x7fffffffffff5fff) (bit-clear 63) (bit-clear 62))
    high = high | 0x0000000000005000L;                    // bit-or
    high = high & 0x7FFFFFFFFFFF5FFFL;                    // bit-and (12 f's, not 13!)
    high = high & ~(1L << 63);                            // bit-clear 63
    high = high & ~(1L << 62);                            // bit-clear 62

    // (-> low (bit-set 63) (bit-clear 62))
    low = low | (1L << 63);                               // bit-set 63
    low = low & ~(1L << 62);                              // bit-clear 62

    return new UUID(high, low);
  }

  /**
   * Hash a map to UUID5 (for commit hashing).
   *
   * <p>Serializes map deterministically by sorting keys and concatenating values. Uses a simple
   * EDN-like representation for compatibility with hasch.
   *
   * @param data Map to hash
   * @return UUID5 of serialized map
   */
  public static UUID hashMap(Map<String, ?> data) {
    // Sort keys for determinism
    TreeMap<String, ?> sorted = new TreeMap<>(data);

    StringBuilder sb = new StringBuilder();
    sb.append("{");

    boolean first = true;
    for (Map.Entry<String, ?> entry : sorted.entrySet()) {
      if (!first) sb.append(", ");
      first = false;

      sb.append(":").append(entry.getKey()).append(" ");

      Object value = entry.getValue();
      if (value == null) {
        sb.append("nil");
      } else if (value instanceof Map) {
        sb.append(serializeMap((Map<?, ?>) value));
      } else if (value instanceof Collection) {
        sb.append(serializeCollection((Collection<?>) value));
      } else {
        sb.append(value.toString());
      }
    }

    sb.append("}");

    return hashBytes(sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Serialize a map for hashing (recursive helper).
   *
   * @param map Map to serialize
   * @return String representation
   */
  private static String serializeMap(Map<?, ?> map) {
    TreeMap<String, Object> sorted = new TreeMap<>();
    for (Map.Entry<?, ?> e : map.entrySet()) {
      sorted.put(e.getKey().toString(), e.getValue());
    }

    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, ?> entry : sorted.entrySet()) {
      if (!first) sb.append(", ");
      first = false;
      sb.append(":").append(entry.getKey()).append(" ");

      Object value = entry.getValue();
      if (value == null) {
        sb.append("nil");
      } else if (value instanceof Map) {
        sb.append(serializeMap((Map<?, ?>) value));
      } else if (value instanceof Collection) {
        sb.append(serializeCollection((Collection<?>) value));
      } else {
        sb.append(value.toString());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Serialize a collection for hashing (recursive helper).
   *
   * @param coll Collection to serialize
   * @return String representation
   */
  private static String serializeCollection(Collection<?> coll) {
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (Object item : coll) {
      if (!first) sb.append(" ");
      first = false;

      if (item == null) {
        sb.append("nil");
      } else if (item instanceof Map) {
        sb.append(serializeMap((Map<?, ?>) item));
      } else if (item instanceof Collection) {
        sb.append(serializeCollection((Collection<?>) item));
      } else {
        sb.append(item.toString());
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Hash all files in a segment.
   *
   * <p>Hashes each file individually and returns a map of filename to hash UUID. Files are hashed
   * in the order provided (use LinkedHashMap for deterministic ordering).
   *
   * @param filePaths Map of filename -> Path
   * @return Map of filename -> hash UUID
   * @throws IOException if any file cannot be read
   */
  public static Map<String, UUID> hashSegmentFiles(Map<String, Path> filePaths) throws IOException {
    Map<String, UUID> hashes = new LinkedHashMap<>();
    for (Map.Entry<String, Path> entry : filePaths.entrySet()) {
      hashes.put(entry.getKey(), hashFile(entry.getValue()));
    }
    return hashes;
  }

  /**
   * Compute commit hash from parent and segment hashes (merkle tree).
   *
   * <p>Creates a content-addressable commit ID by hashing: commit-hash = hash(parent-hash +
   * segment-hashes)
   *
   * <p>This creates a git-like merkle tree where: - Each commit chains to its parent - The commit
   * hash represents all content below it - Tampering with any segment breaks the hash chain
   *
   * @param parentHash Parent commit UUID (or null for root commit)
   * @param segmentHashes Map of segment-name -> (map of filename -> hash)
   * @return Commit merkle hash UUID
   */
  public static UUID computeCommitHash(
      UUID parentHash, Map<String, Map<String, UUID>> segmentHashes) {
    Map<String, Object> commitData = new LinkedHashMap<>();
    commitData.put("parent", parentHash);
    commitData.put("segments", segmentHashes);
    return hashMap(commitData);
  }
}
