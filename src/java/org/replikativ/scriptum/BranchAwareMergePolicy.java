package org.replikativ.scriptum;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

/**
 * A MergePolicy wrapper that prevents merging segments that are all shared across branches.
 *
 * <p>If ALL segments in a proposed merge are shared, merging them would reduce structural sharing
 * and waste disk space. Merges that include at least one branch-specific segment are allowed.
 */
public class BranchAwareMergePolicy extends FilterMergePolicy {

  private volatile Set<String> sharedSegmentNames = Collections.emptySet();

  public BranchAwareMergePolicy(MergePolicy delegate) {
    super(delegate);
  }

  public void setSharedSegmentNames(Set<String> names) {
    this.sharedSegmentNames = Collections.unmodifiableSet(new HashSet<>(names));
  }

  /**
   * Cap the size of a merged segment, in megabytes.
   *
   * <p>Lucene's default is 5 GB, which is tuned for a local disk where a segment is just a file. It
   * is the wrong default for a remote store: a blob is written and read whole, so the largest
   * segment sets the peak memory a commit costs, and konserve's S3 backing holds a blob in the heap
   * to PUT it. A cap of a few hundred MB keeps that bounded and stays well clear of S3's 5 GB
   * single-PUT limit.
   *
   * <p>Merge policies read their settings on each merge decision, so this takes effect immediately
   * and can be changed on a live writer.
   *
   * <p>No-op unless the wrapped policy is a {@link TieredMergePolicy}, which is what
   * {@code BranchIndexWriter} always wraps.
   */
  public void setMaxMergedSegmentMB(double mb) {
    if (in instanceof TieredMergePolicy tiered) {
      tiered.setMaxMergedSegmentMB(mb);
    }
  }

  /** The current merged-segment cap in megabytes, or -1 if the wrapped policy has no such notion. */
  public double getMaxMergedSegmentMB() {
    return (in instanceof TieredMergePolicy tiered) ? tiered.getMaxMergedSegmentMB() : -1.0;
  }

  public synchronized void addSharedSegment(String name) {
    Set<String> updated = new HashSet<>(sharedSegmentNames);
    updated.add(name);
    this.sharedSegmentNames = Collections.unmodifiableSet(updated);
  }

  public synchronized void removeSharedSegment(String name) {
    Set<String> updated = new HashSet<>(sharedSegmentNames);
    updated.remove(name);
    this.sharedSegmentNames = Collections.unmodifiableSet(updated);
  }

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    MergeSpecification spec = super.findMerges(mergeTrigger, segmentInfos, mergeContext);
    return filterMerges(spec);
  }

  @Override
  public MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    MergeSpecification spec =
        super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
    return filterMerges(spec);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    MergeSpecification spec = super.findForcedDeletesMerges(segmentInfos, mergeContext);
    return filterMerges(spec);
  }

  @Override
  public MergeSpecification findFullFlushMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    MergeSpecification spec =
        super.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
    return filterMerges(spec);
  }

  private MergeSpecification filterMerges(MergeSpecification spec) {
    Set<String> shared = this.sharedSegmentNames;
    if (spec == null || shared.isEmpty()) {
      return spec;
    }

    MergeSpecification filtered = new MergeSpecification();
    for (OneMerge merge : spec.merges) {
      boolean hasNonShared = false;
      for (SegmentCommitInfo sci : merge.segments) {
        if (!shared.contains(sci.info.name)) {
          hasNonShared = true;
          break;
        }
      }
      if (hasNonShared) {
        filtered.add(merge);
      }
    }

    return filtered.merges.isEmpty() ? null : filtered;
  }

  @Override
  public String toString() {
    return "BranchAwareMergePolicy("
        + in
        + ", sharedSegments="
        + sharedSegmentNames.size()
        + ")";
  }
}
