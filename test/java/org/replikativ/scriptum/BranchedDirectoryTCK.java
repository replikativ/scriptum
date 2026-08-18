package org.replikativ.scriptum;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;

/**
 * The same Lucene conformance suite, run against the path-based {@link BranchedDirectory}.
 *
 * <p>Here for comparison rather than symmetry: the konserve-backed Directory is new and the
 * overlay-based one is the incumbent, so "is the new one as solid as what it would replace?" is
 * only answerable if both are held to the same contract. A base directory that starts empty is the
 * representative case — a freshly forked branch shares nothing until its parent has committed.
 */
public class BranchedDirectoryTCK extends BaseDirectoryTestCase {

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    Path base = path.resolve("base");
    Path overlay = path.resolve("branches").resolve("tck");
    Files.createDirectories(base);
    Files.createDirectories(overlay);
    return new BranchedDirectory(MMapDirectory.open(base), MMapDirectory.open(overlay), "tck");
  }
}
