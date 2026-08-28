package org.replikativ.scriptum;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.IndexCommit;

/**
 * A {@link BranchDeletionPolicy} for detached generations whose history is owned by an external
 * root graph.
 *
 * <p>Normal Scriptum branches retain every Lucene commit point so native forks can share and
 * revisit them. A detached generation has a different owner: its embedder retains old states by
 * keeping their immutable snapshot addresses. Carrying every old {@code segments_N} into the new
 * snapshot duplicates that history and prevents the embedder's collector from reclaiming it.
 *
 * <p>This policy keeps the newest Lucene commit only, while remaining a {@code
 * BranchDeletionPolicy} so {@link BranchIndexWriter}'s commit metadata and generation accessors
 * keep one implementation.
 */
public final class LatestOnlyBranchDeletionPolicy extends BranchDeletionPolicy {

  private List<? extends IndexCommit> retainLatest(List<? extends IndexCommit> commits)
      throws IOException {
    if (commits.isEmpty()) {
      return Collections.emptyList();
    }

    int last = commits.size() - 1;
    for (int i = 0; i < last; i++) {
      commits.get(i).delete();
    }
    return Collections.singletonList(commits.get(last));
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    super.onInit(retainLatest(commits));
  }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    super.onCommit(retainLatest(commits));
  }
}
