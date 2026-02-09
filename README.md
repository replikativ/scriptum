# Scriptum

[![Clojars](https://img.shields.io/clojars/v/org.replikativ/scriptum.svg)](https://clojars.org/org.replikativ/scriptum)

Copy-on-write branching for Apache Lucene. Git-like snapshot and branch semantics on full-text search indices with structural sharing.

Built on Lucene 10.3.2. Forking a branch takes 3-5ms regardless of index size by sharing immutable segment files.

## Core Concepts

- **Branch**: A COW overlay directory sharing base segments with trunk. Each branch has its own commit history.
- **Snapshot**: An immutable reader at a specific commit generation. All commits are retained until explicit GC.
- **Fork**: Creates a new branch by copying segment metadata only (not data). Near-instant regardless of index size.
- **GC**: Explicit garbage collection of old snapshots, respecting branch references to shared segments.

## Cryptographic Hashing (Optional)

Scriptum supports **optional SHA-512 merkle tree hashing** for content-addressable commits and tamper detection:

- **Dual UUID System**: Each commit has both a random `commit-id` (Lucene internal) and a `content-hash` (merkle root)
- **Content-Hash**: Computed as `hash(parent-content-hash + all-segment-file-hashes)` for complete integrity verification
- **Tamper Detection**: Any file modification breaks the merkle chain
- **Parent Chaining**: Content-hashes link to parent's content-hash (not commit-id) for true merkle tree
- **Yggdrasil Integration**: Content-hash can be used as snapshot-id for distributed systems

Enable with `:crypto-hash? true` when creating an index. Metadata stored in external `scriptum-hashes/*.json` files.

## API Layers

| Layer | Namespace | Use Case |
|-------|-----------|----------|
| **Java** | `org.replikativ.scriptum.BranchIndexWriter` | Direct Java usage |
| **Core** | `scriptum.core` | Low-level Clojure wrapper |
| **Yggdrasil** | `scriptum.yggdrasil` | High-level protocols |

For Clojure users: `scriptum.yggdrasil` for high-level API, `scriptum.core` for lower-level control.

For Java users: use `BranchIndexWriter` directly.

## Getting Started

### Dependencies

Add to `deps.edn`: [![Clojars](https://img.shields.io/clojars/v/org.replikativ/scriptum.svg)](https://clojars.org/org.replikativ/scriptum)

For Maven/Gradle:
```xml
<dependency>
  <groupId>org.replikativ</groupId>
  <artifactId>scriptum</artifactId>
  <version>0.1.1</version>
</dependency>
```

### Build from Source

Java sources must be compiled before use:

```bash
clj -T:build compile-java
```

### Quick Start (Clojure)

```clojure
(require '[scriptum.core :as sc])

;; Create an index (with optional crypto-hash for content-addressable commits)
(def writer (sc/create-index "/tmp/my-index" "main" {:crypto-hash? true}))

;; Add documents
(sc/add-doc writer {:title {:type :text :value "Hello World"}
                    :id    {:type :string :value "doc-1"}})

;; Commit returns detailed info when crypto-hash enabled
(sc/commit! writer "Initial commit")
;; => {:generation 4
;;     :commit-id "27e31528-909b-4bdc-a287-57ed2cec1e6a"
;;     :content-hash "0903b0d6-418c-55b3-9e6b-2c910704edeb"}

;; Verify commit integrity
(sc/verify-commit writer)
;; => {:valid? true, :commit-id "...", :errors []}

;; Search
(sc/search writer {:match-all {}} 10)
;; => [{:title "Hello World", :id "doc-1", :score 1.0}]

;; Fork a branch
(def feature (sc/fork writer "experiment"))

;; Add to branch (doesn't affect main)
(sc/add-doc feature {:title {:type :text :value "Branch only"}
                     :id    {:type :string :value "doc-2"}})
(sc/commit! feature "Added experimental doc")

;; Main still has 1 doc, branch has 2
(count (sc/search writer {:match-all {}} 100))    ;; => 1
(count (sc/search feature {:match-all {}} 100))   ;; => 2

;; Merge branch back
(sc/merge-from! writer feature)
(sc/commit! writer "Merged experiment")

;; Cleanup
(sc/close! feature)
(sc/close! writer)
```

## API Reference

### Lifecycle

```clojure
(sc/create-index path branch-name)                    ; create new index at path
(sc/create-index path branch-name {:crypto-hash? true}) ; with content-addressable commits
(sc/open-branch path branch-name)                     ; open existing branch
(sc/fork writer "branch-name")                        ; fast fork from writer
(sc/close! writer)                                    ; close writer and release resources
(sc/discover-branches path)                           ; => ["main" "feature" ...]

;; Accessors
(sc/num-docs writer)                ; document count (excluding deletions)
(sc/max-doc writer)                 ; document count (including deletions)
(sc/branch-name writer)             ; current branch name
(sc/base-path writer)               ; index base path
(sc/main-branch? writer)            ; true if this is the main branch
```

### Document Operations

Field types: `:text` (analyzed, searchable), `:string` (exact match), `:vector` (float array for KNN).

```clojure
(sc/add-doc writer {:title {:type :text :value "Searchable text"}
                    :tag   {:type :string :value "exact-match"}
                    :embed {:type :vector :value (float-array [0.1 0.2 0.3])
                            :dims 3}})

(sc/delete-docs writer :id "doc-1")           ; delete by field+value
(sc/update-doc writer :id "doc-1" new-fields) ; atomic delete+add
```

### Commit & History

```clojure
;; Commit (returns generation number by default)
(sc/commit! writer "commit message")    ; => 4

;; With crypto-hash enabled, returns detailed map
(sc/commit! writer "commit message")
;; => {:generation 4
;;     :commit-id "27e31528-..."
;;     :content-hash "0903b0d6-..."}  ; merkle root

(sc/flush! writer)                      ; flush without new commit point
(sc/merge-from! writer source-writer)   ; merge segments from another branch

(sc/list-snapshots writer)
;; => [{:generation 1 :uuid "..." :timestamp "..." :message "..." :branch "main"}
;;     {:generation 2 :uuid "..." :timestamp "..." :message "..." :branch "main"}]
```

### Cryptographic Verification

When `:crypto-hash?` is enabled, you can verify commit integrity:

```clojure
;; Verify current commit
(sc/verify-commit writer)
;; => {:valid? true
;;     :commit-id "27e31528-909b-4bdc-a287-57ed2cec1e6a"
;;     :errors []}

;; Verify specific generation
(sc/verify-commit writer {:generation 5})
;; => {:valid? false
;;     :commit-id "..."
;;     :errors ["Segment file not found: _0.cfs"]}

;; Extract content-hash from commit result
(let [{:keys [content-hash]} (sc/commit! writer "msg")]
  (println "Snapshot ID:" content-hash))  ; Use as snapshot-id in Yggdrasil
```

**Note**: Verification recomputes hashes of all segment files and compares with stored metadata. Returns `:valid? true` only if all hashes match.

### Search

```clojure
;; Term query
(sc/search writer {:term {:field "tag" :value "exact-match"}} 10)

;; Match-all
(sc/search writer {:match-all {}} 100)

;; Custom Lucene query object
(sc/search writer my-lucene-query 10)

;; Returns: [{:field1 "val" :field2 "val" :score 1.0} ...]
```

### Time Travel

```clojure
;; Get snapshot at specific generation
(def reader (sc/open-reader-at writer 1))

;; Check if a generation still exists (may be GC'd)
(sc/commit-available? writer 1)  ; => true/false

;; Get current immutable snapshot
(def snap (sc/snapshot writer))

;; Execute with auto-closing snapshot
(sc/with-snapshot [reader writer]
  (sc/search reader {:match-all {}} 10))

(.close reader)
```

### Garbage Collection

```clojure
;; Remove commits older than 1 hour, respecting branch references
(sc/gc! writer)
```

GC only runs on the main branch and protects all segment files referenced by any branch.

## Java API

For Java users, `BranchIndexWriter` provides the complete API:

```java
import org.replikativ.scriptum.BranchIndexWriter;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

// Create an index (with optional crypto-hash)
BranchIndexWriter main = BranchIndexWriter.create(
    Path.of("/tmp/my-index"),
    "main",
    new StandardAnalyzer(),
    true  // enable crypto-hash for content-addressable commits
);

// Add documents
Document doc = new Document();
doc.add(new TextField("title", "Hello World", Field.Store.YES));
doc.add(new StringField("id", "doc-1", Field.Store.YES));
main.addDocument(doc);
main.commit("Initial commit");

// Get content-hash and commit-id
String contentHash = main.getLastContentHash();  // Merkle root (content-addressable)
String commitId = main.getLastCommitId();        // Lucene's random UUID

// Verify commit integrity
Map<String, Object> result = main.verifyCommit(-1);  // -1 = current HEAD
boolean valid = (Boolean) result.get("valid");
if (!valid) {
    List<String> errors = (List<String>) result.get("errors");
    System.err.println("Integrity check failed: " + errors);
}

// Fork a branch (3-5ms regardless of index size)
BranchIndexWriter feature = main.fork("experiment");
feature.addDocument(anotherDoc);
feature.commit("Feature work");

// Search
DirectoryReader reader = main.openReader();
IndexSearcher searcher = new IndexSearcher(reader);
TopDocs results = searcher.search(new MatchAllDocsQuery(), 10);
reader.close();

// Merge branch back
main.mergeFrom(feature);

// Time travel - open reader at specific generation
DirectoryReader historical = main.openReaderAt(1);

// Garbage collect old commits
main.gc(Instant.now().minus(Duration.ofHours(1)));

// Discover branches
Set<String> branches = BranchIndexWriter.discoverBranches(Path.of("/tmp/my-index"));

// Cleanup
feature.close();
main.close();
```

### Key Java Methods

| Method | Description |
|--------|-------------|
| `create(path, branchName, analyzer, cryptoHash)` | Create new index with optional crypto-hash |
| `open(path, branchName)` | Open existing branch |
| `fork(branchName)` | Fast fork (copies metadata only) |
| `addDocument(doc)` | Add a document |
| `deleteDocuments(terms...)` | Delete by terms |
| `updateDocument(term, doc)` | Atomic delete+add |
| `commit()` / `commit(message)` | Persist changes |
| `getLastCommitId()` | Get last commit's UUID (Lucene internal) |
| `getLastContentHash()` | Get last commit's merkle root (if crypto-hash enabled) |
| `verifyCommit(generation)` | Verify commit integrity (returns Map with "valid", "errors") |
| `openReader()` | NRT reader (sees uncommitted) |
| `openCommittedReader()` | Reader on committed state |
| `openReaderAt(generation)` | Time travel to specific commit |
| `isCommitAvailable(generation)` | Check if commit still exists |
| `listSnapshots()` | Get all commit points |
| `mergeFrom(source)` | Merge another branch |
| `gc(beforeInstant)` | Garbage collect old commits |
| `numDocs()` / `maxDoc()` | Document counts |
| `getBranchName()` | Current branch name |
| `isMainBranch()` | Check if main branch |

## Yggdrasil Integration

Scriptum implements the [Yggdrasil](https://github.com/replikativ/yggdrasil) protocol stack (Snapshotable, Branchable, Graphable, Mergeable):

```clojure
(require '[scriptum.yggdrasil :as sy]
         '[yggdrasil.protocols :as p])

(def sys (sy/create "/tmp/my-index" {:system-name "search-index"}))

(p/branches sys)         ; => #{:main}
(p/branch! sys :feature)
(p/checkout sys :feature)
;; ... add docs, commit ...
(p/merge! sys :main)
(p/history sys {:limit 10})

(sy/close! sys)
```

Passes the full yggdrasil compliance test suite (22 tests, 203 assertions).

## Performance

Typical results:
- Fork latency: 3-5ms (independent of index size)
- Indexing: ~50k docs/sec (text fields, SSD)
- Search: sub-millisecond for simple queries

## Directory Layout

On disk, scriptum uses this structure:

```
basePath/                    -- trunk (main branch)
  _0.cfs, _1.cfs, ...       -- shared segment files
  segments_N                 -- main's commit points
  scriptum-hashes/           -- crypto-hash metadata (if enabled)
    <commit-uuid>.json       -- merkle tree data per commit
  branches/
    feature/                 -- branch overlay
      _10000.cfs, ...        -- branch-specific segments
      segments_N             -- branch's commit points
```

Branches share base segments via read-only references. Only new writes create branch-specific segment files.

When crypto-hash is enabled, each commit generates a JSON metadata file containing:
- `content-hash`: The merkle root (content-addressable commit ID)
- `parent-content-hash`: Parent commit's content-hash (for merkle chain)
- `segments`: Map of segment names to file hashes (SHA-512 of each segment file)

## Technical Documentation

See [docs/LUCENE_EXTENSION.md](docs/LUCENE_EXTENSION.md) for a deep-dive into how Scriptum extends Lucene:

- How Lucene segments and commit points work
- BranchedDirectory: overlay pattern for COW reads/writes
- BranchDeletionPolicy: retaining all commits until explicit GC
- BranchAwareMergePolicy: preventing merge of shared segments
- Fork operation mechanics and performance analysis
- GC with branch protection

## Project Structure

```
src/
  clojure/scriptum/
    core.clj                 # Low-level COW branching API
    yggdrasil.clj            # Yggdrasil protocol adapter
  java/org/replikativ/scriptum/
    BranchIndexWriter.java   # Branch-aware Lucene writer (main Java API)
    BranchedDirectory.java   # COW directory overlay
    BranchAwareMergePolicy.java  # Prevents merging shared segments
    BranchDeletionPolicy.java    # Retains all commits until GC
    ContentHash.java         # SHA-512 hashing for merkle trees
docs/
  LUCENE_EXTENSION.md        # Technical deep-dive
test/scriptum/
  core_test.clj              # Unit tests
  crypto_test.clj            # Crypto-hash integrity tests
  yggdrasil_test.clj         # Compliance tests
```

## Requirements

- Java 21+
- Clojure 1.12.0+
- Apache Lucene 10.3.2 (pulled from Maven Central)

## Development

```bash
# Compile Java sources
clj -T:build compile-java

# Run tests
clj -T:build compile-java && clj -M:test

# Start nREPL
clj -T:build compile-java && clj -M:repl
```

## License

Copyright (c) 2026 Christian Weilbach

Licensed under the Apache License, Version 2.0.
