package org.replikativ.scriptum;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

/**
 * A copy-on-write directory that overlays branch-specific writes on a shared base directory.
 *
 * <p>All writes go to the overlay directory. Reads try the overlay first, then fall back to the
 * base. Deletes of base files are recorded but not performed on the base, preserving shared
 * segments for other branches.
 */
public class BranchedDirectory extends Directory {

  private final Directory baseDir;
  private final Directory overlayDir;
  private final String branchName;

  private final Set<String> overlayFiles = ConcurrentHashMap.newKeySet();
  private final Set<String> deletedBaseFiles = ConcurrentHashMap.newKeySet();
  private final AtomicLong tempFileCounter = new AtomicLong(0);

  private volatile boolean closed = false;

  public BranchedDirectory(Directory baseDir, Directory overlayDir, String branchName) {
    this.baseDir = baseDir;
    this.overlayDir = overlayDir;
    this.branchName = branchName;

    try {
      String[] existing = overlayDir.listAll();
      for (String f : existing) {
        overlayFiles.add(f);
      }
    } catch (IOException e) {
      // Overlay may be empty/new
    }
  }

  public Directory getBaseDir() { return baseDir; }
  public Directory getOverlayDir() { return overlayDir; }
  public String getBranchName() { return branchName; }
  public Set<String> getOverlayFileNames() { return Collections.unmodifiableSet(overlayFiles); }
  public Set<String> getDeletedBaseFileNames() { return Collections.unmodifiableSet(deletedBaseFiles); }

  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    TreeSet<String> all = new TreeSet<>();
    for (String f : baseDir.listAll()) {
      if (!deletedBaseFiles.contains(f) && !f.startsWith("segments_")) {
        all.add(f);
      }
    }
    all.addAll(overlayFiles);
    return all.toArray(new String[0]);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    if (overlayFiles.contains(name)) {
      overlayDir.deleteFile(name);
      overlayFiles.remove(name);
    } else {
      boolean exists = false;
      try {
        baseDir.fileLength(name);
        exists = true;
      } catch (NoSuchFileException | FileNotFoundException e) {
        // not in base
      }
      if (!exists) {
        throw new NoSuchFileException(name);
      }
      deletedBaseFiles.add(name);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    if (overlayFiles.contains(name)) {
      return overlayDir.fileLength(name);
    }
    if (deletedBaseFiles.contains(name)) {
      throw new NoSuchFileException(name);
    }
    return baseDir.fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    IndexOutput out = overlayDir.createOutput(name, context);
    overlayFiles.add(name);
    return out;
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    ensureOpen();
    IndexOutput out = overlayDir.createTempOutput(prefix, suffix, context);
    overlayFiles.add(out.getName());
    return out;
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
    Set<String> overlaySync = new HashSet<>();
    for (String name : names) {
      if (overlayFiles.contains(name)) {
        overlaySync.add(name);
      }
    }
    if (!overlaySync.isEmpty()) {
      overlayDir.sync(overlaySync);
    }
  }

  @Override
  public void syncMetaData() throws IOException {
    ensureOpen();
    overlayDir.syncMetaData();
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    ensureOpen();
    if (overlayFiles.contains(source)) {
      overlayDir.rename(source, dest);
      overlayFiles.remove(source);
      overlayFiles.add(dest);
    } else {
      try (IndexInput in = baseDir.openInput(source, IOContext.READONCE);
          IndexOutput out = overlayDir.createOutput(dest, IOContext.DEFAULT)) {
        out.copyBytes(in, in.length());
      }
      overlayFiles.add(dest);
      deletedBaseFiles.add(source);
    }
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    if (overlayFiles.contains(name)) {
      return overlayDir.openInput(name, context);
    }
    if (deletedBaseFiles.contains(name)) {
      throw new NoSuchFileException(name);
    }
    return baseDir.openInput(name, context);
  }

  @Override
  public Lock obtainLock(String name) throws IOException {
    ensureOpen();
    return overlayDir.obtainLock(branchName + "_" + name);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        overlayDir.close();
      } finally {
        baseDir.close();
      }
    }
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    ensureOpen();
    Set<String> pending = new HashSet<>();
    pending.addAll(overlayDir.getPendingDeletions());
    return pending;
  }

  @Override
  protected void ensureOpen() {
    if (closed) {
      throw new org.apache.lucene.store.AlreadyClosedException(
          "BranchedDirectory[" + branchName + "] is closed");
    }
  }

  @Override
  public String toString() {
    return "BranchedDirectory("
        + branchName
        + ", base="
        + baseDir
        + ", overlay="
        + overlayDir
        + ")";
  }
}
