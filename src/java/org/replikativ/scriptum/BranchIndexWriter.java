package org.replikativ.scriptum;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

/**
 * A branch-aware IndexWriter that wraps Lucene's IndexWriter with COW branching semantics.
 *
 * <p>Directory layout:
 *
 * <pre>
 *   basePath/                    -- shared segment files (the "trunk")
 *     _0.cfs, _1.cfs, ...      -- segment files written by main before fork
 *     segments_N               -- main's commit points (all retained until gc)
 *   basePath/branches/exp/      -- experiment branch overlay
 *     _10000.cfs, ...           -- segments written only on experiment
 *     segments_N               -- experiment's commit points
 * </pre>
 *
 * <p>Segment files are never automatically deleted. Cleanup happens only via explicit {@link
 * #gc(Instant)} calls, which check all branch references before removing any files.
 *
 * <p>Each commit stores a timestamp in user-data, enabling snapshot retention with date-based
 * cutoffs.
 *
 * <p>Usage:
 *
 * <pre>
 *   BranchIndexWriter main = BranchIndexWriter.create(basePath, "main");
 *   main.addDocument(doc);
 *   main.commit();  // stores timestamp in commit user-data
 *
 *   BranchIndexWriter experiment = main.fork("experiment");
 *   experiment.addDocument(doc2);
 *   experiment.commit();
 *
 *   // Later: clean up old snapshots
 *   main.gc(Instant.now().minus(Duration.ofDays(7)));
 * </pre>
 */
public class BranchIndexWriter implements Closeable {

  private static final String COMMIT_TIMESTAMP_KEY = "scriptum.timestamp";
  private static final String COMMIT_MESSAGE_KEY = "scriptum.message";
  private static final String COMMIT_BRANCH_KEY = "scriptum.branch";
  private static final String COMMIT_UUID_KEY = "scriptum.uuid";
  private static final String COMMIT_PARENT_IDS_KEY = "scriptum.parent-ids";

  private final IndexWriter writer;
  private final Directory directory;
  private final BranchDeletionPolicy deletionPolicy;
  private final BranchAwareMergePolicy mergePolicy;
  private final String branchName;
  private final Path basePath;
  private final Analyzer analyzer;
  private final boolean isMainBranch;

  // Optional message for the next commit
  private volatile String pendingCommitMessage;

  // UUID of the last commit (for parent-id chain)
  private volatile String lastCommitId;

  // Extra parent UUIDs for merge commits (set before commit, cleared after)
  private volatile String pendingExtraParentIds;

  private BranchIndexWriter(
      IndexWriter writer,
      Directory directory,
      BranchDeletionPolicy deletionPolicy,
      BranchAwareMergePolicy mergePolicy,
      String branchName,
      Path basePath,
      Analyzer analyzer,
      boolean isMainBranch) {
    this.writer = writer;
    this.directory = directory;
    this.deletionPolicy = deletionPolicy;
    this.mergePolicy = mergePolicy;
    this.branchName = branchName;
    this.basePath = basePath;
    this.analyzer = analyzer;
    this.isMainBranch = isMainBranch;
  }

  /** Initialize lastCommitId from the latest existing commit's UUID. */
  private void initLastCommitId() {
    IndexCommit last = deletionPolicy.getLastCommit();
    if (last != null) {
      try {
        Map<String, String> userData = last.getUserData();
        lastCommitId = userData.get(COMMIT_UUID_KEY);
      } catch (IOException e) {
        // If we can't read user-data, leave null (first commit will have no parent)
      }
    }
  }

  /**
   * Create a new BranchIndexWriter at the given path.
   *
   * @param basePath the root path for the index
   * @param branchName the name of the branch (typically "main")
   * @return a new BranchIndexWriter
   */
  public static BranchIndexWriter create(Path basePath, String branchName) throws IOException {
    return create(basePath, branchName, new StandardAnalyzer());
  }

  /**
   * Create a new BranchIndexWriter at the given path with a custom analyzer.
   *
   * <p>On creation, discovers existing branches under basePath/branches/ and configures the merge
   * policy to protect shared segments.
   *
   * @param basePath the root path for the index
   * @param branchName the name of the branch
   * @param analyzer the analyzer to use for text processing
   * @return a new BranchIndexWriter
   */
  public static BranchIndexWriter create(Path basePath, String branchName, Analyzer analyzer)
      throws IOException {
    Files.createDirectories(basePath);

    Directory dir = MMapDirectory.open(basePath);

    BranchDeletionPolicy deletionPolicy = new BranchDeletionPolicy();
    BranchAwareMergePolicy mergePolicy =
        new BranchAwareMergePolicy(new org.apache.lucene.index.TieredMergePolicy());

    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setIndexDeletionPolicy(deletionPolicy);
    config.setMergePolicy(mergePolicy);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

    IndexWriter writer = new IndexWriter(dir, config);

    BranchIndexWriter biw =
        new BranchIndexWriter(
            writer, dir, deletionPolicy, mergePolicy, branchName, basePath, analyzer, true);

    // Initialize parent tracking from existing commits
    biw.initLastCommitId();

    // Discover existing branches and protect their shared segments
    biw.discoverAndProtectBranches();

    return biw;
  }

  /**
   * Open an existing branch writer (non-main branch).
   *
   * <p>Opens a BranchedDirectory with the base as read-only and the overlay for writes.
   *
   * @param basePath the root path of the index
   * @param branchName the name of the branch to open
   * @param analyzer the analyzer to use
   * @return a BranchIndexWriter for the branch
   */
  public static BranchIndexWriter open(Path basePath, String branchName, Analyzer analyzer)
      throws IOException {
    Path branchPath = basePath.resolve("branches").resolve(branchName);
    if (!Files.isDirectory(branchPath)) {
      throw new IOException("Branch directory not found: " + branchPath);
    }

    Directory baseDir = MMapDirectory.open(basePath);
    Directory overlayDir = MMapDirectory.open(branchPath);
    BranchedDirectory branchDir = new BranchedDirectory(baseDir, overlayDir, branchName);

    BranchDeletionPolicy deletionPolicy = new BranchDeletionPolicy();
    BranchAwareMergePolicy mergePolicy =
        new BranchAwareMergePolicy(new org.apache.lucene.index.TieredMergePolicy());

    // Determine shared segments (those in base that this branch references)
    SegmentInfos branchInfos = SegmentInfos.readLatestCommit(branchDir);
    Set<String> sharedNames = new HashSet<>();
    for (SegmentCommitInfo sci : branchInfos) {
      // A segment is shared if its files exist in the base directory
      try {
        baseDir.fileLength(sci.info.name + ".cfs");
        sharedNames.add(sci.info.name);
      } catch (IOException e) {
        // Check individual files for non-compound segments
        try {
          baseDir.fileLength(sci.info.name + ".si");
          sharedNames.add(sci.info.name);
        } catch (IOException e2) {
          // Not in base, branch-specific
        }
      }
    }
    mergePolicy.setSharedSegmentNames(sharedNames);

    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setIndexDeletionPolicy(deletionPolicy);
    config.setMergePolicy(mergePolicy);
    config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

    IndexWriter writer = new IndexWriter(branchDir, config);

    BranchIndexWriter biw =
        new BranchIndexWriter(
            writer, branchDir, deletionPolicy, mergePolicy, branchName, basePath, analyzer, false);
    biw.initLastCommitId();
    return biw;
  }

  /** Convenience overload with StandardAnalyzer. */
  public static BranchIndexWriter open(Path basePath, String branchName) throws IOException {
    return open(basePath, branchName, new StandardAnalyzer());
  }

  /**
   * Discover existing branches and configure the merge policy to protect shared segments.
   *
   * <p>Scans basePath/branches/ for branch directories, reads their segments_N, and marks segments
   * that are referenced by any branch as shared (preventing merge of all-shared segments).
   */
  private void discoverAndProtectBranches() throws IOException {
    Path branchesDir = basePath.resolve("branches");
    if (!Files.isDirectory(branchesDir)) {
      return;
    }

    Set<String> allSharedNames = new HashSet<>();

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(branchesDir)) {
      for (Path branchPath : stream) {
        if (!Files.isDirectory(branchPath)) {
          continue;
        }
        try {
          String bName = branchPath.getFileName().toString();
          try (Directory baseDir = MMapDirectory.open(basePath);
              Directory overlayDir = MMapDirectory.open(branchPath);
              BranchedDirectory branchDir = new BranchedDirectory(baseDir, overlayDir, bName)) {
            SegmentInfos branchInfos = SegmentInfos.readLatestCommit(branchDir);
            for (SegmentCommitInfo sci : branchInfos) {
              allSharedNames.add(sci.info.name);
            }
          }
        } catch (IOException e) {
          // Branch may be empty or corrupted, skip
        }
      }
    }

    if (!allSharedNames.isEmpty()) {
      mergePolicy.setSharedSegmentNames(allSharedNames);
    }
  }

  /**
   * Discover all branch names at the given base path.
   *
   * @param basePath the root path of the index
   * @return set of branch names (directory names under basePath/branches/)
   */
  public static Set<String> discoverBranches(Path basePath) throws IOException {
    Set<String> branches = new HashSet<>();
    Path branchesDir = basePath.resolve("branches");
    if (!Files.isDirectory(branchesDir)) {
      return branches;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(branchesDir)) {
      for (Path branchPath : stream) {
        if (Files.isDirectory(branchPath)) {
          branches.add(branchPath.getFileName().toString());
        }
      }
    }
    return branches;
  }

  /**
   * Fork this branch into a new branch. The new branch shares all existing segments.
   *
   * <p>Cost: ~3-5ms (flush + copy manifest).
   *
   * @param newBranchName the name of the new branch
   * @return a new BranchIndexWriter for the forked branch
   */
  public synchronized BranchIndexWriter fork(String newBranchName) throws IOException {
    // 1. Flush and commit current state
    writer.flush();
    String forkUuid = setCommitData("Fork point for " + newBranchName);
    writer.commit();
    lastCommitId = forkUuid;

    // 2. Read current SegmentInfos
    SegmentInfos currentInfos = SegmentInfos.readLatestCommit(directory);

    // 3. Create new overlay directory
    Path newOverlayPath = basePath.resolve("branches").resolve(newBranchName);
    Files.createDirectories(newOverlayPath);

    Directory baseDir = MMapDirectory.open(basePath);
    Directory overlayDir = MMapDirectory.open(newOverlayPath);
    BranchedDirectory newBranchDir = new BranchedDirectory(baseDir, overlayDir, newBranchName);

    // 4. Clone SegmentInfos and bump counter to avoid segment name collisions
    SegmentInfos cloned = currentInfos.clone();
    cloned.counter = currentInfos.counter + 10000;

    // 5. Write cloned SegmentInfos to new branch
    cloned.commit(newBranchDir);

    // 6. Track shared segments for merge policy (on both sides)
    Set<String> sharedNames = new HashSet<>();
    for (SegmentCommitInfo sci : currentInfos) {
      sharedNames.add(sci.info.name);
    }
    mergePolicy.setSharedSegmentNames(sharedNames);

    // 7. Create new writer on the fork
    BranchDeletionPolicy newDeletionPolicy = new BranchDeletionPolicy();
    BranchAwareMergePolicy newMergePolicy =
        new BranchAwareMergePolicy(new org.apache.lucene.index.TieredMergePolicy());
    newMergePolicy.setSharedSegmentNames(sharedNames);

    IndexWriterConfig newConfig = new IndexWriterConfig(analyzer);
    newConfig.setIndexDeletionPolicy(newDeletionPolicy);
    newConfig.setMergePolicy(newMergePolicy);
    newConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

    IndexWriter newWriter = new IndexWriter(newBranchDir, newConfig);

    BranchIndexWriter forked =
        new BranchIndexWriter(
            newWriter, newBranchDir, newDeletionPolicy, newMergePolicy, newBranchName, basePath,
            analyzer, false);
    forked.lastCommitId = this.lastCommitId;
    return forked;
  }

  /**
   * Add a document to this branch.
   *
   * @param doc the document to add
   * @return the sequence number for this operation
   */
  public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
    return writer.addDocument(doc);
  }

  /**
   * Delete documents matching the given terms.
   *
   * @param terms the terms identifying documents to delete
   * @return the sequence number for this operation
   */
  public long deleteDocuments(Term... terms) throws IOException {
    return writer.deleteDocuments(terms);
  }

  /**
   * Delete documents matching the given queries.
   *
   * @param queries the queries identifying documents to delete
   * @return the sequence number for this operation
   */
  public long deleteDocuments(Query... queries) throws IOException {
    return writer.deleteDocuments(queries);
  }

  /**
   * Update a document identified by the given term.
   *
   * @param term the term identifying the document to update
   * @param doc the new document
   * @return the sequence number for this operation
   */
  public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
    return writer.updateDocument(term, doc);
  }

  /**
   * Set a message for the next commit (stored in commit user-data).
   *
   * @param message the commit message
   */
  public void setCommitMessage(String message) {
    this.pendingCommitMessage = message;
  }

  /**
   * Set commit user-data (timestamp + message + branch + parents) on the writer.
   */
  private String setCommitData(String message) {
    String uuid = UUID.randomUUID().toString();
    Map<String, String> userData = new LinkedHashMap<>();
    userData.put(COMMIT_UUID_KEY, uuid);
    userData.put(COMMIT_TIMESTAMP_KEY, Instant.now().toString());
    userData.put(COMMIT_BRANCH_KEY, branchName);
    if (message != null && !message.isEmpty()) {
      userData.put(COMMIT_MESSAGE_KEY, message);
    }
    StringBuilder parentIds = new StringBuilder();
    if (lastCommitId != null) {
      parentIds.append(lastCommitId);
    }
    if (pendingExtraParentIds != null) {
      if (parentIds.length() > 0) {
        parentIds.append(",");
      }
      parentIds.append(pendingExtraParentIds);
      pendingExtraParentIds = null;
    }
    if (parentIds.length() > 0) {
      userData.put(COMMIT_PARENT_IDS_KEY, parentIds.toString());
    }
    writer.setLiveCommitData(userData.entrySet());
    return uuid;
  }

  /**
   * Commit all pending changes to this branch.
   *
   * @return the commit generation
   */
  public long commit() throws IOException {
    String uuid = setCommitData(pendingCommitMessage);
    pendingCommitMessage = null;
    long gen = writer.commit();
    lastCommitId = uuid;
    return gen;
  }

  /**
   * Commit with a message.
   *
   * @param message the commit message
   * @return the commit generation
   */
  public long commit(String message) throws IOException {
    String uuid = setCommitData(message);
    pendingCommitMessage = null;
    long gen = writer.commit();
    lastCommitId = uuid;
    return gen;
  }

  /** Flush pending changes without committing. */
  public void flush() throws IOException {
    writer.flush();
  }

  /**
   * Force merge all segments down to maxNumSegments.
   *
   * @param maxNumSegments target number of segments
   */
  public void forceMerge(int maxNumSegments) throws IOException {
    writer.forceMerge(maxNumSegments);
  }

  /**
   * Open a reader on this branch for searching (NRT - sees uncommitted changes).
   *
   * @return a DirectoryReader on this branch
   */
  public DirectoryReader openReader() throws IOException {
    return DirectoryReader.open(writer);
  }

  /**
   * Open a reader that sees only committed changes.
   *
   * @return a DirectoryReader on this branch's committed state
   */
  public DirectoryReader openCommittedReader() throws IOException {
    return DirectoryReader.open(directory);
  }

  /**
   * Open a reader at a specific commit point (snapshot/time-travel).
   *
   * @param generation the commit generation to open
   * @return a DirectoryReader at that commit point
   * @throws IOException if the generation is not found (may have been GC'd)
   */
  public DirectoryReader openReaderAt(long generation) throws IOException {
    for (IndexCommit commit : deletionPolicy.getAllCommits()) {
      if (commit.getGeneration() == generation) {
        return DirectoryReader.open(commit);
      }
    }
    throw new IOException(
        "Commit generation " + generation + " not found (may have been garbage collected)");
  }

  /**
   * Check if a specific commit generation is still available (not GC'd).
   *
   * @param generation the commit generation to check
   * @return true if the commit is still retained, false if it has been GC'd
   */
  public boolean isCommitAvailable(long generation) {
    for (IndexCommit commit : deletionPolicy.getAllCommits()) {
      if (commit.getGeneration() == generation) {
        return true;
      }
    }
    return false;
  }

  /**
   * List all available snapshots (commit points) for this branch.
   *
   * @return list of snapshot info maps (oldest to newest)
   */
  public List<Map<String, Object>> listSnapshots() throws IOException {
    List<Map<String, Object>> snapshots = new ArrayList<>();
    for (IndexCommit commit : deletionPolicy.getAllCommits()) {
      Map<String, Object> info = new LinkedHashMap<>();
      info.put("generation", commit.getGeneration());
      info.put("segmentCount", commit.getSegmentCount());

      Map<String, String> userData = commit.getUserData();
      if (userData.containsKey(COMMIT_UUID_KEY)) {
        info.put("snapshotId", userData.get(COMMIT_UUID_KEY));
      }
      if (userData.containsKey(COMMIT_TIMESTAMP_KEY)) {
        info.put("timestamp", userData.get(COMMIT_TIMESTAMP_KEY));
      }
      if (userData.containsKey(COMMIT_MESSAGE_KEY)) {
        info.put("message", userData.get(COMMIT_MESSAGE_KEY));
      }
      if (userData.containsKey(COMMIT_BRANCH_KEY)) {
        info.put("branch", userData.get(COMMIT_BRANCH_KEY));
      }
      if (userData.containsKey(COMMIT_PARENT_IDS_KEY)) {
        info.put("parentIds", userData.get(COMMIT_PARENT_IDS_KEY));
      }

      snapshots.add(info);
    }
    return snapshots;
  }

  /**
   * Garbage collect old commit points and unreferenced segment files.
   *
   * <p>Only callable on the main branch (which owns the base directory).
   *
   * @param before delete commits with timestamps before this instant
   * @return number of commit points removed
   */
  public int gc(Instant before) throws IOException {
    if (!isMainBranch) {
      throw new IOException("gc() can only be called on the main branch writer");
    }

    Set<String> protectedFiles = collectBranchReferencedFiles(before);
    deletionPolicy.setGcCutoff(before, protectedFiles);

    String gcUuid = setCommitData("GC pass");
    writer.commit();
    lastCommitId = gcUuid;

    return deletionPolicy.getLastGcDeleted();
  }

  /**
   * Collect segment files referenced by branch commits within the grace window.
   */
  private Set<String> collectBranchReferencedFiles(Instant before) throws IOException {
    Set<String> protectedFiles = new HashSet<>();
    Path branchesDir = basePath.resolve("branches");
    if (!Files.isDirectory(branchesDir)) {
      return protectedFiles;
    }

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(branchesDir)) {
      for (Path branchPath : stream) {
        if (!Files.isDirectory(branchPath)) {
          continue;
        }
        try {
          String bName = branchPath.getFileName().toString();
          try (Directory baseDir = MMapDirectory.open(basePath);
              Directory overlayDir = MMapDirectory.open(branchPath);
              BranchedDirectory branchDir = new BranchedDirectory(baseDir, overlayDir, bName)) {

            String[] overlayFiles = overlayDir.listAll();
            for (String fileName : overlayFiles) {
              if (!fileName.startsWith("segments_")) {
                continue;
              }
              try {
                SegmentInfos infos = SegmentInfos.readCommit(branchDir, fileName);

                boolean protect = true;
                String tsStr = infos.getUserData().get(COMMIT_TIMESTAMP_KEY);
                if (tsStr != null) {
                  try {
                    Instant commitTime = Instant.parse(tsStr);
                    protect = !commitTime.isBefore(before);
                  } catch (Exception e) {
                    // Unparseable timestamp = protect (conservative)
                  }
                }

                if (protect) {
                  for (SegmentCommitInfo sci : infos) {
                    protectedFiles.addAll(sci.files());
                  }
                }
              } catch (IOException e) {
                // Corrupted commit point, skip
              }
            }
          }
        } catch (IOException e) {
          // Branch may be corrupted; be conservative, skip
        }
      }
    }

    return protectedFiles;
  }

  /**
   * Merge segments from a source branch into this branch (add-only merge).
   *
   * @param source the source branch to merge from
   */
  public void mergeFrom(BranchIndexWriter source) throws IOException {
    source.commit("Pre-merge snapshot");
    commit("Pre-merge snapshot");
    try (DirectoryReader reader = DirectoryReader.open(source.directory)) {
      List<CodecReader> codecReaders = new ArrayList<>();
      for (LeafReaderContext ctx : reader.leaves()) {
        codecReaders.add((CodecReader) ctx.reader());
      }
      writer.addIndexes(codecReaders.toArray(new CodecReader[0]));
    }
    pendingExtraParentIds = source.lastCommitId;
    commit("Merged from " + source.branchName);
  }

  /** Returns the UUID of the last commit, or null if no commits yet. */
  public String getLastCommitId() {
    return lastCommitId;
  }

  /** Returns the branch name. */
  public String getBranchName() {
    return branchName;
  }

  /** Returns the underlying Directory. */
  public Directory getDirectory() {
    return directory;
  }

  /** Returns the underlying IndexWriter. */
  public IndexWriter getIndexWriter() {
    return writer;
  }

  /** Returns the base path of the index. */
  public Path getBasePath() {
    return basePath;
  }

  /** Returns the number of documents in this branch (including deletions). */
  public int maxDoc() {
    return writer.getDocStats().maxDoc;
  }

  /** Returns the number of documents in this branch (excluding deletions). */
  public int numDocs() {
    return writer.getDocStats().numDocs;
  }

  /** Returns true if the writer is open. */
  public boolean isOpen() {
    return writer.isOpen();
  }

  /** Returns true if this is the main (trunk) branch. */
  public boolean isMainBranch() {
    return isMainBranch;
  }

  /** Returns the deletion policy for this writer. */
  public BranchDeletionPolicy getDeletionPolicy() {
    return deletionPolicy;
  }

  @Override
  public void close() throws IOException {
    writer.close();
    directory.close();
  }

  @Override
  public String toString() {
    return "BranchIndexWriter("
        + branchName
        + ", docs="
        + numDocs()
        + ", base="
        + basePath
        + ")";
  }
}
