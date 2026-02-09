package org.replikativ.scriptum;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
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

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final String COMMIT_TIMESTAMP_KEY = "scriptum.timestamp";
  private static final String COMMIT_MESSAGE_KEY = "scriptum.message";
  private static final String COMMIT_BRANCH_KEY = "scriptum.branch";
  private static final String COMMIT_UUID_KEY = "scriptum.uuid";
  private static final String COMMIT_PARENT_IDS_KEY = "scriptum.parent-ids";
  private static final String COMMIT_CRYPTO_HASH_KEY = "scriptum.crypto-hash";
  private static final String HASH_METADATA_DIR = "scriptum-hashes";

  private final IndexWriter writer;
  private final Directory directory;
  private final BranchDeletionPolicy deletionPolicy;
  private final BranchAwareMergePolicy mergePolicy;
  private final String branchName;
  private final Path basePath;
  private final Analyzer analyzer;
  private final boolean isMainBranch;
  private final boolean cryptoHash;

  // Optional message for the next commit
  private volatile String pendingCommitMessage;

  // UUID of the last commit (for parent-id chain)
  private volatile String lastCommitId;
  private volatile String lastContentHash; // Merkle root hash (when crypto-hash enabled)

  // Extra parent UUIDs for merge commits (set before commit, cleared after)
  private volatile String pendingExtraParentIds;

  // Cache of segment name -> file hashes (for crypto-hash mode)
  private final Map<String, Map<String, UUID>> segmentHashCache;

  // In-memory index of custom metadata key -> (value -> generation) for O(log n) lookups.
  // Lazily populated on first query by scanning all commits once, then maintained incrementally.
  private final Map<String, NavigableMap<String, Long>> metadataIndex = new HashMap<>();
  private volatile boolean metadataIndexInitialized = false;

  private BranchIndexWriter(
      IndexWriter writer,
      Directory directory,
      BranchDeletionPolicy deletionPolicy,
      BranchAwareMergePolicy mergePolicy,
      String branchName,
      Path basePath,
      Analyzer analyzer,
      boolean isMainBranch,
      boolean cryptoHash) {
    this.writer = writer;
    this.directory = directory;
    this.deletionPolicy = deletionPolicy;
    this.mergePolicy = mergePolicy;
    this.branchName = branchName;
    this.basePath = basePath;
    this.analyzer = analyzer;
    this.isMainBranch = isMainBranch;
    this.cryptoHash = cryptoHash;
    this.segmentHashCache = cryptoHash ? new java.util.concurrent.ConcurrentHashMap<>() : null;
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
    return create(basePath, branchName, new StandardAnalyzer(), false);
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
    return create(basePath, branchName, analyzer, false);
  }

  /**
   * Create a new BranchIndexWriter with optional crypto-hash support.
   *
   * <p>When cryptoHash is true, commits use content-addressable merkle hashing where the commit
   * UUID is derived from the hash of all segment file contents plus the parent commit hash. This
   * enables tamper detection and auditability.
   *
   * @param basePath the root path for the index
   * @param branchName the name of the branch
   * @param analyzer the analyzer to use for text processing
   * @param cryptoHash if true, use merkle hashing for commits
   * @return a new BranchIndexWriter
   */
  public static BranchIndexWriter create(
      Path basePath, String branchName, Analyzer analyzer, boolean cryptoHash) throws IOException {
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
            writer, dir, deletionPolicy, mergePolicy, branchName, basePath, analyzer, true,
            cryptoHash);

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
    try {
      Directory overlayDir = MMapDirectory.open(branchPath);
      try {
        BranchedDirectory branchDir = new BranchedDirectory(baseDir, overlayDir, branchName);
        // branchDir now owns baseDir and overlayDir (closes them in its close())

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

        IndexWriter writer;
        try {
          writer = new IndexWriter(branchDir, config);
        } catch (IOException e) {
          branchDir.close();
          throw e;
        }

        // Detect crypto-hash from existing commits
        boolean cryptoHash = false;
        try {
          Map<String, String> userData = branchInfos.getUserData();
          cryptoHash = "true".equals(userData.get(COMMIT_CRYPTO_HASH_KEY));
        } catch (Exception e) {
          // No user data or error reading, default to false
        }

        BranchIndexWriter biw =
            new BranchIndexWriter(
                writer, branchDir, deletionPolicy, mergePolicy, branchName, basePath, analyzer,
                false, cryptoHash);
        biw.initLastCommitId();
        return biw;
      } catch (IOException e) {
        overlayDir.close();
        throw e;
      }
    } catch (IOException e) {
      baseDir.close();
      throw e;
    }
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
    try {
      Directory overlayDir = MMapDirectory.open(newOverlayPath);
      try {
        BranchedDirectory newBranchDir = new BranchedDirectory(baseDir, overlayDir, newBranchName);
        // newBranchDir now owns baseDir and overlayDir

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

        IndexWriter newWriter;
        try {
          newWriter = new IndexWriter(newBranchDir, newConfig);
        } catch (IOException e) {
          newBranchDir.close();
          throw e;
        }

        BranchIndexWriter forked =
            new BranchIndexWriter(
                newWriter, newBranchDir, newDeletionPolicy, newMergePolicy, newBranchName, basePath,
                analyzer, false, this.cryptoHash);
        forked.lastCommitId = this.lastCommitId;
        return forked;
      } catch (IOException e) {
        overlayDir.close();
        throw e;
      }
    } catch (IOException e) {
      baseDir.close();
      throw e;
    }
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
   * Set commit user-data (timestamp + message + branch + parents + custom metadata) on the writer.
   */
  private String setCommitData(String message) throws IOException {
    return setCommitData(message, null);
  }

  /**
   * Set commit user-data with optional custom metadata entries.
   *
   * <p>Custom metadata keys must NOT use the "scriptum." prefix (reserved for internal use).
   *
   * @param message optional commit message
   * @param customMetadata optional map of custom key-value pairs to store in commit user-data
   * @return the generated commit UUID
   */
  private String setCommitData(String message, Map<String, String> customMetadata)
      throws IOException {
    String uuid = UUID.randomUUID().toString();
    Map<String, String> userData = new LinkedHashMap<>();

    userData.put(COMMIT_UUID_KEY, uuid);
    userData.put(COMMIT_TIMESTAMP_KEY, Instant.now().toString());
    userData.put(COMMIT_BRANCH_KEY, branchName);
    if (cryptoHash) {
      userData.put(COMMIT_CRYPTO_HASH_KEY, "true");
    }
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
    // Merge custom metadata (after internal keys, so internal keys take precedence)
    if (customMetadata != null) {
      for (Map.Entry<String, String> entry : customMetadata.entrySet()) {
        String key = entry.getKey();
        if (!key.startsWith("scriptum.")) {
          userData.put(key, entry.getValue());
        }
      }
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
    return commit(pendingCommitMessage, null);
  }

  /**
   * Commit with a message.
   *
   * @param message the commit message
   * @return the commit generation
   */
  public long commit(String message) throws IOException {
    return commit(message, null);
  }

  /**
   * Commit with a message and custom metadata.
   *
   * <p>Custom metadata entries are stored in the commit's user-data alongside internal fields.
   * Keys must NOT use the "scriptum." prefix (reserved for internal use). Values must be strings.
   *
   * <p>Example use case: storing a database transaction ID for secondary index synchronization.
   *
   * @param message the commit message (may be null)
   * @param customMetadata optional map of custom key-value pairs (may be null)
   * @return the commit generation
   */
  public long commit(String message, Map<String, String> customMetadata) throws IOException {
    if (cryptoHash) {
      long gen = commitWithCryptoHash(message, customMetadata);
      updateMetadataIndex(customMetadata, gen);
      return gen;
    } else {
      String uuid = setCommitData(message, customMetadata);
      pendingCommitMessage = null;
      long gen = writer.commit();
      lastCommitId = uuid;
      updateMetadataIndex(customMetadata, gen);
      return gen;
    }
  }

  /**
   * Commit with crypto-hash: first commit normally, then hash segments and store metadata.
   *
   * @param message commit message
   * @param customMetadata optional custom metadata
   * @return commit generation
   */
  private long commitWithCryptoHash(String message, Map<String, String> customMetadata)
      throws IOException {
    // Save previous commit ID for parent chain lookup BEFORE overwriting
    String previousCommitId = lastCommitId;

    // Phase 1: Commit with Lucene's random UUID
    String luceneUuid = setCommitData(message, customMetadata);
    pendingCommitMessage = null;
    long gen = writer.commit();
    lastCommitId = luceneUuid;

    // Phase 2: Compute complete merkle root
    try {
      SegmentInfos infos = SegmentInfos.readLatestCommit(directory);

      // Step 1: Hash all segment files (leaf level of merkle tree)
      Map<String, Map<String, UUID>> segmentHashes = new HashMap<>();
      for (SegmentCommitInfo sci : infos) {
        Map<String, UUID> fileHashes = getSegmentFileHashes(sci);
        if (fileHashes != null) {
          segmentHashes.put(sci.info.name, fileHashes);
        }
      }

      // Step 2: Get parent's content-hash (not Lucene's random UUID!)
      UUID parentContentHash = loadParentContentHash(previousCommitId);

      // Step 3: Compute merkle root = hash(parent-hash + all-segment-hashes)
      UUID contentHash = ContentHash.computeCommitHash(parentContentHash, segmentHashes);

      // Step 4: Store for API access
      lastContentHash = contentHash.toString();

      // Step 5: Store the mapping and merkle tree
      storeContentHashMetadata(luceneUuid, contentHash, segmentHashes, parentContentHash);

    } catch (IOException e) {
      // Log error but don't fail the commit - the Lucene commit already succeeded
      System.err.println("Warning: Failed to store crypto-hash metadata: " + e.getMessage());
      lastContentHash = null;
    }

    return gen;
  }

  /**
   * Load the content-hash of the parent commit (for merkle chain).
   *
   * @param parentCommitId the Lucene UUID of the parent commit
   * @return parent's content-hash UUID, or null if no parent or parent didn't have crypto-hash
   */
  private UUID loadParentContentHash(String parentCommitId) throws IOException {
    if (parentCommitId == null) {
      return null; // Root commit, no parent
    }

    // Load previous commit's metadata
    Map<String, Object> parentMetadata = loadSegmentHashMetadata(parentCommitId);
    if (parentMetadata == null) {
      return null; // Parent didn't have crypto-hash enabled
    }

    // Get the content-hash (NOT the lucene-id!)
    String contentHashStr = (String) parentMetadata.get("content-hash");
    if (contentHashStr == null) {
      return null; // Old format without content-hash
    }

    return UUID.fromString(contentHashStr);
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
   * <p>Uses binary search on the sorted commit list for O(log n) lookup.
   *
   * @param generation the commit generation to open
   * @return a DirectoryReader at that commit point
   * @throws IOException if the generation is not found (may have been GC'd)
   */
  public DirectoryReader openReaderAt(long generation) throws IOException {
    IndexCommit commit = findCommitByGeneration(generation);
    if (commit == null) {
      throw new IOException(
          "Commit generation " + generation + " not found (may have been garbage collected)");
    }
    return DirectoryReader.open(commit);
  }

  /**
   * Check if a specific commit generation is still available (not GC'd).
   *
   * <p>Uses binary search for O(log n) lookup.
   *
   * @param generation the commit generation to check
   * @return true if the commit is still retained, false if it has been GC'd
   */
  public boolean isCommitAvailable(long generation) {
    return findCommitByGeneration(generation) != null;
  }

  /**
   * Binary search for a commit by generation in the sorted commit list.
   *
   * <p>The commit list from BranchDeletionPolicy is guaranteed to be sorted by generation
   * (ascending), so binary search is safe and gives O(log n) lookup.
   *
   * @param generation the commit generation to find
   * @return the IndexCommit, or null if not found
   */
  private IndexCommit findCommitByGeneration(long generation) {
    List<IndexCommit> commits = deletionPolicy.getAllCommits();
    int lo = 0, hi = commits.size() - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      long midGen = commits.get(mid).getGeneration();
      if (midGen < generation) {
        lo = mid + 1;
      } else if (midGen > generation) {
        hi = mid - 1;
      } else {
        return commits.get(mid);
      }
    }
    return null;
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

      // Expose custom (non-scriptum.) metadata
      Map<String, String> customData = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : userData.entrySet()) {
        if (!entry.getKey().startsWith("scriptum.")) {
          customData.put(entry.getKey(), entry.getValue());
        }
      }
      if (!customData.isEmpty()) {
        info.put("customMetadata", customData);
      }

      snapshots.add(info);
    }
    return snapshots;
  }

  // ============================================================================
  // Custom Metadata Index — O(log n) lookup for T→generation mapping
  // ============================================================================

  /**
   * Lazily build the in-memory metadata index from all existing commits.
   *
   * <p>Scans all commits once (O(n)), reading userData from each. Subsequent commits are indexed
   * incrementally in setCommitData(). After GC, the index is rebuilt to remove stale entries.
   */
  private synchronized void ensureMetadataIndex() throws IOException {
    if (metadataIndexInitialized) {
      return;
    }
    metadataIndex.clear();
    for (IndexCommit commit : deletionPolicy.getAllCommits()) {
      Map<String, String> userData = commit.getUserData();
      long gen = commit.getGeneration();
      for (Map.Entry<String, String> entry : userData.entrySet()) {
        if (!entry.getKey().startsWith("scriptum.")) {
          metadataIndex
              .computeIfAbsent(entry.getKey(), k -> new TreeMap<>())
              .put(entry.getValue(), gen);
        }
      }
    }
    metadataIndexInitialized = true;
  }

  /**
   * Incrementally update the metadata index after a commit.
   *
   * @param customMetadata the custom metadata from the commit (may be null)
   * @param generation the generation of the commit
   */
  private void updateMetadataIndex(Map<String, String> customMetadata, long generation) {
    if (customMetadata == null || !metadataIndexInitialized) {
      return;
    }
    synchronized (this) {
      for (Map.Entry<String, String> entry : customMetadata.entrySet()) {
        if (!entry.getKey().startsWith("scriptum.")) {
          metadataIndex
              .computeIfAbsent(entry.getKey(), k -> new TreeMap<>())
              .put(entry.getValue(), generation);
        }
      }
    }
  }

  /**
   * Find the commit generation for an exact custom metadata value.
   *
   * <p>O(log n) lookup using the in-memory metadata index.
   *
   * @param key the custom metadata key (e.g. "datahike/tx")
   * @param value the exact value to find
   * @return the commit generation, or -1 if not found
   */
  public long findGenerationByMetadata(String key, String value) throws IOException {
    ensureMetadataIndex();
    NavigableMap<String, Long> index = metadataIndex.get(key);
    if (index == null) {
      return -1;
    }
    Long gen = index.get(value);
    return gen != null ? gen : -1;
  }

  /**
   * Find the commit generation for a custom metadata value using floor lookup.
   *
   * <p>Returns the generation of the latest commit whose metadata value for the given key is
   * less than or equal to the target value (using natural string ordering). This is ideal for
   * monotonically increasing values like transaction IDs.
   *
   * <p>O(log n) lookup using the in-memory metadata index.
   *
   * @param key the custom metadata key (e.g. "datahike/tx")
   * @param value the target value (finds the latest entry &lt;= this value)
   * @return a map with "generation" and "indexedValue", or null if no matching commit exists
   */
  public Map<String, Object> findGenerationFloorByMetadata(String key, String value)
      throws IOException {
    ensureMetadataIndex();
    NavigableMap<String, Long> index = metadataIndex.get(key);
    if (index == null) {
      return null;
    }
    Map.Entry<String, Long> floor = index.floorEntry(value);
    if (floor == null) {
      return null;
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("generation", floor.getValue());
    result.put("indexedValue", floor.getKey());
    return result;
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

    Set<String> protectedCommitIds = new HashSet<>();
    Set<String> protectedFiles = collectBranchReferencedFiles(before, protectedCommitIds);
    deletionPolicy.setGcCutoff(before, protectedFiles);

    // Use crypto-hash commit path so GC commit gets proper metadata
    if (cryptoHash) {
      commitWithCryptoHash("GC pass", null);
    } else {
      String gcUuid = setCommitData("GC pass");
      writer.commit();
      lastCommitId = gcUuid;
    }

    // Collect surviving main branch commit IDs for hash metadata protection
    if (cryptoHash) {
      for (IndexCommit commit : deletionPolicy.getAllCommits()) {
        try {
          String uuid = commit.getUserData().get(COMMIT_UUID_KEY);
          if (uuid != null) {
            protectedCommitIds.add(uuid);
          }
        } catch (IOException e) {
          // Skip unreadable commits
        }
      }
      gcHashMetadataFiles(protectedCommitIds);
    }

    // Invalidate metadata index so it's rebuilt on next query
    metadataIndexInitialized = false;

    return deletionPolicy.getLastGcDeleted();
  }

  /**
   * Collect segment files and commit IDs referenced by branch commits within the grace window.
   *
   * @param before the cutoff timestamp
   * @param protectedCommitIds output set to populate with protected commit UUIDs
   * @return set of protected segment filenames
   */
  private Set<String> collectBranchReferencedFiles(Instant before, Set<String> protectedCommitIds)
      throws IOException {
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
                  // Track commit UUID for hash metadata cleanup
                  String commitUuid = infos.getUserData().get(COMMIT_UUID_KEY);
                  if (commitUuid != null) {
                    protectedCommitIds.add(commitUuid);
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

  /**
   * Get the content-hash (merkle root) of the last commit.
   *
   * @return content-hash UUID as string, or null if crypto-hash not enabled or no commits yet
   */
  public String getLastContentHash() {
    return lastContentHash;
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

  // ============================================================================
  // Crypto-hash Support
  // ============================================================================

  /**
   * Get or compute hashes for all files in a segment.
   *
   * <p>Uses cache to avoid rehashing shared segments from parent commits.
   */
  private Map<String, UUID> getSegmentFileHashes(SegmentCommitInfo sci) throws IOException {
    if (!cryptoHash) {
      return null;
    }

    String segmentName = sci.info.name;

    return segmentHashCache.computeIfAbsent(
        segmentName,
        k -> {
          try {
            // Check if we have this in parent commit
            Map<String, UUID> parentHash = lookupParentSegmentHash(segmentName);
            if (parentHash != null) {
              return parentHash; // Reuse from parent (immutable segment)
            }

            // New segment - compute hash
            Map<String, Path> filePaths = new LinkedHashMap<>();
            for (String fileName : sci.files()) {
              // For main branch, files are in basePath directly
              // For other branches, check branch overlay first, then basePath
              Path filePath;
              if (isMainBranch) {
                filePath = basePath.resolve(fileName);
              } else {
                filePath = basePath.resolve("branches").resolve(branchName).resolve(fileName);
                if (!Files.exists(filePath)) {
                  // Try base directory for shared segments
                  filePath = basePath.resolve(fileName);
                }
              }

              if (Files.exists(filePath)) {
                filePaths.put(fileName, filePath);
              }
            }

            return ContentHash.hashSegmentFiles(filePaths);
          } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        });
  }

  /**
   * Look up segment hashes from parent commit.
   *
   * @return cached hashes if segment exists in parent, null otherwise
   */
  private Map<String, UUID> lookupParentSegmentHash(String segmentName) {
    if (lastCommitId == null) {
      return null;
    }

    try {
      Map<String, Object> metadata = loadSegmentHashMetadata(lastCommitId);
      if (metadata == null) {
        return null;
      }

      @SuppressWarnings("unchecked")
      Map<String, Map<String, UUID>> parentHashes =
          (Map<String, Map<String, UUID>>) metadata.get("segments");
      if (parentHashes == null) {
        return null;
      }

      return parentHashes.get(segmentName);
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Store content-hash metadata including merkle root and segment hashes.
   *
   * @param luceneUuid Lucene's random commit UUID
   * @param contentHash The merkle root hash (content-addressable)
   * @param segmentHashes Map of segment name -> file hashes
   * @param parentContentHash Parent commit's content-hash (or null for root)
   */
  private void storeContentHashMetadata(
      String luceneUuid,
      UUID contentHash,
      Map<String, Map<String, UUID>> segmentHashes,
      UUID parentContentHash)
      throws IOException {
    Path hashDir = basePath.resolve(HASH_METADATA_DIR);
    Files.createDirectories(hashDir);

    Path hashFile = hashDir.resolve(luceneUuid + ".json");

    // Build complete metadata JSON
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("lucene-id", luceneUuid);
    metadata.put("content-hash", contentHash.toString());
    metadata.put("parent-content-hash", parentContentHash != null ? parentContentHash.toString() : null);
    metadata.put("timestamp", Instant.now().toString());
    metadata.put("segments", segmentHashes);

    String json = encodeMetadataJson(metadata);
    Files.writeString(hashFile, json, StandardCharsets.UTF_8);
  }

  /**
   * Load content-hash metadata from external file.
   *
   * @return metadata map with keys: lucene-id, content-hash, parent-content-hash, timestamp, segments
   */
  private Map<String, Object> loadSegmentHashMetadata(String commitUuid) throws IOException {
    Path hashFile = basePath.resolve(HASH_METADATA_DIR).resolve(commitUuid + ".json");

    if (!Files.exists(hashFile)) {
      return null;
    }

    String json = Files.readString(hashFile, StandardCharsets.UTF_8);
    return decodeMetadataJson(json);
  }

  /**
   * Encode complete metadata to JSON using Jackson.
   *
   * <p>Converts UUID values to strings for JSON serialization.
   */
  private String encodeMetadataJson(Map<String, Object> metadata) throws IOException {
    // Convert UUID values to strings for JSON
    Map<String, Object> jsonMap = new LinkedHashMap<>();
    jsonMap.put("lucene-id", metadata.get("lucene-id"));
    jsonMap.put("content-hash", String.valueOf(metadata.get("content-hash")));

    Object parentHash = metadata.get("parent-content-hash");
    jsonMap.put("parent-content-hash", parentHash != null ? parentHash.toString() : null);

    jsonMap.put("timestamp", String.valueOf(metadata.get("timestamp")));

    // Convert segment hashes: Map<String, Map<String, UUID>> -> Map<String, Map<String, String>>
    @SuppressWarnings("unchecked")
    Map<String, Map<String, UUID>> segments =
        (Map<String, Map<String, UUID>>) metadata.get("segments");
    Map<String, Map<String, String>> segStrings = new LinkedHashMap<>();
    if (segments != null) {
      for (Map.Entry<String, Map<String, UUID>> segEntry : segments.entrySet()) {
        Map<String, String> fileStrings = new LinkedHashMap<>();
        for (Map.Entry<String, UUID> fileEntry : segEntry.getValue().entrySet()) {
          fileStrings.put(fileEntry.getKey(), fileEntry.getValue().toString());
        }
        segStrings.put(segEntry.getKey(), fileStrings);
      }
    }
    jsonMap.put("segments", segStrings);

    return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(jsonMap);
  }

  /**
   * Decode complete metadata from JSON using Jackson.
   *
   * <p>Converts string values back to UUIDs for segment hashes.
   */
  private Map<String, Object> decodeMetadataJson(String json) throws IOException {
    Map<String, Object> raw =
        JSON.readValue(json, new TypeReference<Map<String, Object>>() {});

    Map<String, Object> result = new HashMap<>();

    if (raw.containsKey("lucene-id")) result.put("lucene-id", raw.get("lucene-id"));
    if (raw.containsKey("content-hash")) result.put("content-hash", raw.get("content-hash"));
    if (raw.containsKey("timestamp")) result.put("timestamp", raw.get("timestamp"));

    Object parentHash = raw.get("parent-content-hash");
    if (parentHash != null) {
      result.put("parent-content-hash", parentHash.toString());
    }

    // Convert segments: Map<String, Map<String, String>> -> Map<String, Map<String, UUID>>
    @SuppressWarnings("unchecked")
    Map<String, Map<String, String>> segStrings =
        (Map<String, Map<String, String>>) raw.get("segments");
    if (segStrings != null) {
      Map<String, Map<String, UUID>> segments = new HashMap<>();
      for (Map.Entry<String, Map<String, String>> segEntry : segStrings.entrySet()) {
        Map<String, UUID> fileHashes = new HashMap<>();
        for (Map.Entry<String, String> fileEntry : segEntry.getValue().entrySet()) {
          fileHashes.put(fileEntry.getKey(), UUID.fromString(fileEntry.getValue()));
        }
        segments.put(segEntry.getKey(), fileHashes);
      }
      result.put("segments", segments);
    }

    return result;
  }

  /**
   * Cleanup old hash metadata files during GC.
   *
   * @param protectedCommitIds Set of commit UUIDs to protect from deletion
   */
  private void gcHashMetadataFiles(Set<String> protectedCommitIds) throws IOException {
    if (!cryptoHash) {
      return;
    }

    Path hashDir = basePath.resolve(HASH_METADATA_DIR);
    if (!Files.exists(hashDir)) {
      return;
    }

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(hashDir, "*.json")) {
      for (Path hashFile : stream) {
        String fileName = hashFile.getFileName().toString();
        // Extract UUID from filename: 550e8400-....json
        String commitId = fileName.substring(0, fileName.length() - 5);

        if (!protectedCommitIds.contains(commitId)) {
          Files.delete(hashFile);
        }
      }
    }
  }

  /**
   * Verify the integrity of a commit by recomputing its merkle hash.
   *
   * @param generation The commit generation to verify (-1 for current HEAD)
   * @return Map with verification results: {valid: boolean, commitId: String, errors: List<String>}
   * @throws IOException if commit cannot be read or crypto-hash is not enabled
   */
  public Map<String, Object> verifyCommit(long generation) throws IOException {
    if (!cryptoHash) {
      throw new IllegalStateException("Crypto-hash is not enabled for this index");
    }

    Map<String, Object> result = new HashMap<>();
    List<String> errors = new ArrayList<>();

    // Get the commit to verify
    SegmentInfos infos;
    if (generation < 0) {
      infos = SegmentInfos.readLatestCommit(directory);
    } else {
      List<IndexCommit> commits = DirectoryReader.listCommits(directory);
      IndexCommit commit = commits.stream()
          .filter(c -> c.getGeneration() == generation)
          .findFirst()
          .orElseThrow(() -> new IOException("Commit generation " + generation + " not found"));
      infos = SegmentInfos.readCommit(directory, commit.getSegmentsFileName());
    }

    Map<String, String> userData = infos.getUserData();
    String commitUuid = userData.get(COMMIT_UUID_KEY);
    result.put("commitId", commitUuid);

    // Load stored metadata
    Map<String, Object> metadata = loadSegmentHashMetadata(commitUuid);
    if (metadata == null) {
      errors.add("Hash metadata file not found for commit " + commitUuid);
      result.put("valid", false);
      result.put("errors", errors);
      return result;
    }

    @SuppressWarnings("unchecked")
    Map<String, Map<String, UUID>> storedHashes =
        (Map<String, Map<String, UUID>>) metadata.get("segments");
    if (storedHashes == null) {
      storedHashes = new HashMap<>();
    }

    // Recompute segment hashes
    Map<String, Map<String, UUID>> computedHashes = new HashMap<>();

    for (SegmentCommitInfo sci : infos) {
      String segName = sci.info.name;
      Map<String, Path> filePaths = new LinkedHashMap<>();

      for (String fileName : sci.files()) {
        // For main branch, files are in basePath directly
        // For other branches, check branch overlay first, then basePath
        Path filePath;
        if (isMainBranch) {
          filePath = basePath.resolve(fileName);
        } else {
          filePath = basePath.resolve("branches").resolve(branchName).resolve(fileName);
          if (!Files.exists(filePath)) {
            // Try base directory for shared segments
            filePath = basePath.resolve(fileName);
          }
        }

        if (Files.exists(filePath)) {
          filePaths.put(fileName, filePath);
        } else {
          errors.add("Segment file not found: " + fileName);
        }
      }

      if (!filePaths.isEmpty()) {
        try {
          Map<String, UUID> fileHashes = ContentHash.hashSegmentFiles(filePaths);
          computedHashes.put(segName, fileHashes);
        } catch (IOException e) {
          errors.add("Failed to hash segment " + segName + ": " + e.getMessage());
        }
      }
    }

    // Compare stored vs computed hashes
    for (Map.Entry<String, Map<String, UUID>> entry : storedHashes.entrySet()) {
      String segName = entry.getKey();
      Map<String, UUID> storedSegHashes = entry.getValue();
      Map<String, UUID> computedSegHashes = computedHashes.get(segName);

      if (computedSegHashes == null) {
        errors.add("Segment " + segName + " missing in current commit");
        continue;
      }

      for (Map.Entry<String, UUID> fileEntry : storedSegHashes.entrySet()) {
        String fileName = fileEntry.getKey();
        UUID storedHash = fileEntry.getValue();
        UUID computedHash = computedSegHashes.get(fileName);

        if (computedHash == null) {
          errors.add("File " + fileName + " in segment " + segName + " is missing");
        } else if (!storedHash.equals(computedHash)) {
          errors.add(
              "Hash mismatch for "
                  + fileName
                  + " in segment "
                  + segName
                  + ": expected "
                  + storedHash
                  + ", got "
                  + computedHash);
        }
      }
    }

    // Note: Commit UUID is random (not content-addressed) in current implementation
    // Verification only checks that segment hashes match stored metadata

    result.put("valid", errors.isEmpty());
    result.put("errors", errors);
    return result;
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
