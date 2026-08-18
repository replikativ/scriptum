package org.replikativ.scriptum;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

/**
 * An IndexDeletionPolicy that keeps ALL commit points alive until explicit GC.
 *
 * <p>This implements the "never-delete" model for COW branching: segment files accumulate until
 * explicit GC is performed via {@link #setGcCutoff}.
 */
public class BranchDeletionPolicy extends IndexDeletionPolicy {

  private static final String TIMESTAMP_KEY = "scriptum.timestamp";

  private static final String COMMIT_UUID_KEY = "scriptum.uuid";

  /**
   * Immutable holder for GC parameters, set atomically via volatile.
   *
   * <p>Two ways to name what goes, because the two callers ask different questions. A time
   * {@code cutoff} answers "drop history older than this", which is what {@code scriptum.core/gc!}
   * and yggdrasil's {@code :remove-before} express. An explicit {@code commitIds} set answers
   * "drop exactly these", which is what yggdrasil's coordinator hands an adapter: it computes
   * reachability itself from {@code gc-roots} and the commit graph, then passes each system its
   * own candidates. A cutoff cannot express that — an unreachable commit may be newer than a
   * reachable one on another branch.
   */
  private static final class GcConfig {
    final Instant cutoff;
    final Set<String> commitIds;
    final Set<String> keepIds;
    final Set<String> protectedFiles;

    GcConfig(
        Instant cutoff, Set<String> commitIds, Set<String> keepIds, Set<String> protectedFiles) {
      this.cutoff = cutoff;
      this.commitIds = commitIds;
      this.keepIds = keepIds;
      this.protectedFiles = protectedFiles;
    }

    /** Whether this config selects `commit` for deletion. */
    boolean selects(IndexCommit commit) throws IOException {
      String uuid = commit.getUserData().get(COMMIT_UUID_KEY);
      // A branch head is never dropped, however old. The directory-backed
      // collector protects every branch's head for the same reason and says so
      // at length; a time cutoff alone dropped the head that `gc-roots` had
      // just reported as live, because the collector's own new commit became
      // the newest and the real head no longer was.
      if (keepIds != null && keepIds.contains(uuid)) {
        return false;
      }
      // OR, not AND, when both are given: "drop these, AND ALSO anything older
      // than this". The two layers disagreed — the caller's precheck ignored the
      // cutoff whenever ids were present while this ANDed them — so a sweep given
      // both committed (the precheck said something would go) and then dropped
      // nothing (a candidate newer than the cutoff failed the AND). That is the
      // collector growing the index while reclaiming nothing, and the coordinator
      // deregistering candidates it believes were deleted.
      boolean byId = commitIds != null && commitIds.contains(uuid);
      boolean byAge = cutoff != null && isBeforeCutoff(commit, cutoff);
      return byId || byAge;
    }

    private static boolean isBeforeCutoff(IndexCommit commit, Instant cutoff) throws IOException {
      String ts = commit.getUserData().get(TIMESTAMP_KEY);
      return ts != null && Instant.parse(ts).isBefore(cutoff);
    }
  }

  private volatile List<IndexCommit> commitSnapshot = Collections.emptyList();
  private volatile GcConfig pendingGc;
  private volatile int lastGcDeleted;

  public BranchDeletionPolicy() {}

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    commitSnapshot = Collections.unmodifiableList(new ArrayList<>(commits));
  }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    GcConfig gc = this.pendingGc;
    if (gc != null) {
      lastGcDeleted = 0;
      List<IndexCommit> surviving = new ArrayList<>();
      for (int i = 0; i < commits.size(); i++) {
        IndexCommit commit = commits.get(i);
        boolean isLast = (i == commits.size() - 1);
        if (!isLast
            && gc.selects(commit)
            && !hasProtectedFiles(commit, gc.protectedFiles)) {
          commit.delete();
          lastGcDeleted++;
        } else {
          surviving.add(commit);
        }
      }
      commitSnapshot = Collections.unmodifiableList(surviving);
      this.pendingGc = null;
    } else {
      commitSnapshot = Collections.unmodifiableList(new ArrayList<>(commits));
    }
  }

  public void setGcCutoff(Instant before, Set<String> protectedFiles) {
    this.pendingGc =
        new GcConfig(
            before, null, null, Collections.unmodifiableSet(new HashSet<>(protectedFiles)));
  }

  /**
   * Drop exactly these commit points, by {@code scriptum.uuid}, on the next commit.
   *
   * <p>For a coordinator that has already decided what is unreachable — yggdrasil computes the
   * reachable set from every system's {@code gc-roots} and the commit graph, then hands each
   * adapter its own candidates. The newest commit is never dropped whatever is asked, because it
   * is the front index state.
   *
   * @param commitIds {@code scriptum.uuid} values to delete
   * @param protectedFiles files that must survive regardless
   */
  public void setGcCommits(Set<String> commitIds, Set<String> protectedFiles) {
    this.pendingGc =
        new GcConfig(
            null,
            Collections.unmodifiableSet(new HashSet<>(commitIds)),
            null,
            Collections.unmodifiableSet(new HashSet<>(protectedFiles)));
  }

  /**
   * Arm a collection that additionally never drops {@code keepIds}, whatever else selects them.
   *
   * @param before drop commit points older than this, or null when using {@code commitIds}
   * @param commitIds drop exactly these {@code scriptum.uuid}s, or null when using {@code before}
   * @param keepIds {@code scriptum.uuid}s that must survive regardless — branch heads
   */
  public void setGc(Instant before, Set<String> commitIds, Set<String> keepIds) {
    this.pendingGc =
        new GcConfig(
            before,
            commitIds == null ? null : Collections.unmodifiableSet(new HashSet<>(commitIds)),
            keepIds == null ? null : Collections.unmodifiableSet(new HashSet<>(keepIds)),
            Collections.emptySet());
  }

  public int getLastGcDeleted() {
    return lastGcDeleted;
  }

  private boolean isBeforeCutoff(IndexCommit commit, Instant cutoff) throws IOException {
    Map<String, String> userData = commit.getUserData();
    String tsStr = userData.get(TIMESTAMP_KEY);
    if (tsStr == null) {
      return false;
    }
    try {
      Instant commitTime = Instant.parse(tsStr);
      return commitTime.isBefore(cutoff);
    } catch (Exception e) {
      return false;
    }
  }

  private boolean hasProtectedFiles(IndexCommit commit, Set<String> protectedFiles)
      throws IOException {
    for (String file : commit.getFileNames()) {
      if (protectedFiles.contains(file)) {
        return true;
      }
    }
    return false;
  }

  public List<IndexCommit> getAllCommits() {
    return commitSnapshot;
  }

  public IndexCommit getLastCommit() {
    List<IndexCommit> snapshot = commitSnapshot;
    if (snapshot.isEmpty()) {
      return null;
    }
    return snapshot.get(snapshot.size() - 1);
  }

  public int getCommitCount() {
    return commitSnapshot.size();
  }
}
