package org.replikativ.scriptum;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;

/**
 * Lucene's own Directory conformance suite, run against the konserve-backed Directory.
 *
 * <p>{@link BaseDirectoryTestCase} is the contract {@code MMapDirectory} and friends are held to.
 * Pointing it at {@code scriptum.konserve} answers the question that hand-written tests cannot: is
 * a store-backed Directory actually a Directory, or only enough of one for the happy path Lucene
 * itself happens to take?
 *
 * <p>The Directory is built in Clojure ({@code scriptum.tck/directory-for}) rather than here,
 * because constructing a konserve store from Java means reimplementing keyword-argument handling
 * for no benefit.
 */
@ThreadLeakFilters(filters = {ScriptumDirectoryTCK.ClojureAsyncThreads.class})
public class ScriptumDirectoryTCK extends BaseDirectoryTestCase {

  /**
   * core.async's dispatch pool outlives the suite by design — konserve's async API is built on it
   * and it is process-wide, not per-store. Reported as a leak because the suite cannot know that;
   * filtered because a shared thread pool staying alive is not a Directory defect.
   */
  public static class ClojureAsyncThreads implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      String name = t.getName();
      return name.startsWith("async-dispatch-") || name.startsWith("async-thread-macro-");
    }
  }

  private static final IFn DIRECTORY_FOR;

  static {
    IFn require = Clojure.var("clojure.core", "require");
    require.invoke(Clojure.read("scriptum.tck"));
    DIRECTORY_FOR = Clojure.var("scriptum.tck", "directory-for");
  }

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return (Directory) DIRECTORY_FOR.invoke(path.toString());
  }
}
