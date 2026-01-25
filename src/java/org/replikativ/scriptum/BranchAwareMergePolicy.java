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
