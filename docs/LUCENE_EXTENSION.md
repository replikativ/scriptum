# Scriptum: Lucene Extension Design

This document describes how Scriptum extends Apache Lucene to provide copy-on-write branching with Git-like semantics.

## Overview

Scriptum achieves **zero-cost branching** on Lucene indices by:

- Sharing immutable segment files between branches
- Using overlay directories for branch-specific writes
- Retaining all commit points until explicit garbage collection
- Preventing merge operations that would break structural sharing

The extension consists of four Java classes:

| Class | Purpose |
|-------|---------|
| `BranchedDirectory` | COW overlay: reads fall back to base, writes go to overlay |
| `BranchDeletionPolicy` | Retains all commits until explicit GC |
| `BranchAwareMergePolicy` | Prevents merging shared-only segments |
| `BranchIndexWriter` | Main API: create, fork, commit, gc |

---

## Lucene Background

To understand Scriptum, you need to understand how Lucene stores data.

### Segments

Lucene stores an index as a collection of **segments**. Each segment is an immutable mini-index containing:

```
_0.cfs          -- Compound file containing all segment data
_0.si           -- Segment info metadata
_0_Lucene101_0.doc  -- Or individual files if not compound
```

When you add documents, Lucene buffers them in memory, then flushes to a new segment. Old segments are never modified - new data always creates new segments.

### SegmentInfos (segments_N)

The `segments_N` file is the **commit point** - a manifest listing which segments comprise the index:

```
segments_5:
  ├── segment _0  (1000 docs)
  ├── segment _1  (500 docs)
  ├── segment _2  (200 docs)
  └── user-data: {timestamp: "...", message: "..."}
```

Each commit creates a new `segments_N` file (incrementing N). The segment files themselves don't change - only the manifest changes.

### Merge

Lucene periodically merges small segments into larger ones for search efficiency. Merge creates a **new** segment and deletes the old ones. This is where the default behavior breaks branching - merged segments would invalidate branches still referencing the originals.

### Deletion Policy

Lucene's `IndexDeletionPolicy` controls when old commit points and unreferenced segments are deleted. The default (`KeepOnlyLastCommitDeletionPolicy`) immediately deletes all but the latest commit.

---

## Key Innovation: BranchedDirectory

`BranchedDirectory` implements copy-on-write at the directory level:

```
┌─────────────────────────────────────────────────────────────────┐
│                    BRANCHED DIRECTORY                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   READ OPERATION                    WRITE OPERATION              │
│   ──────────────                    ───────────────              │
│   1. Check overlay                  Always goes to overlay       │
│   2. If not found → base            (never touches base)         │
│   3. Serve from wherever found                                   │
│                                                                  │
│   DELETE OPERATION                                               │
│   ────────────────                                               │
│   Overlay file → actually delete                                 │
│   Base file → record in deletedBaseFiles set (soft delete)      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Directory Layout

```
basePath/                       ← Main branch (trunk)
  ├── _0.cfs                    ← Segment files (shared)
  ├── _1.cfs
  ├── _2.cfs
  ├── segments_1                ← Main's commit points
  ├── segments_2
  └── branches/
      └── experiment/           ← Branch overlay directory
          ├── _10000.cfs        ← Branch-only segment
          ├── _10001.cfs
          ├── segments_1        ← Branch's commit points
          └── segments_2
```

### How Reads Work

When a branch opens `_1.cfs`:

```java
@Override
public IndexInput openInput(String name, IOContext context) throws IOException {
    // 1. Check overlay first
    if (overlayFiles.contains(name)) {
        return overlayDir.openInput(name, context);
    }
    // 2. Check if "deleted" (soft-deleted base file)
    if (deletedBaseFiles.contains(name)) {
        throw new NoSuchFileException(name);
    }
    // 3. Fall back to base (shared segments)
    return baseDir.openInput(name, context);
}
```

### How Writes Work

All writes go to the overlay - base is read-only:

```java
@Override
public IndexOutput createOutput(String name, IOContext context) throws IOException {
    IndexOutput out = overlayDir.createOutput(name, context);
    overlayFiles.add(name);  // Track for future reads
    return out;
}
```

### Soft Deletion

When a branch "deletes" a base file, it's only recorded - the file stays for other branches:

```java
@Override
public void deleteFile(String name) throws IOException {
    if (overlayFiles.contains(name)) {
        overlayDir.deleteFile(name);    // Actually delete overlay files
        overlayFiles.remove(name);
    } else {
        deletedBaseFiles.add(name);     // Just record for base files
    }
}
```

---

## BranchDeletionPolicy: Never-Delete Model

Standard Lucene deletes old commit points immediately. Scriptum keeps them all:

```
┌─────────────────────────────────────────────────────────────────┐
│                    COMMIT RETENTION                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   STANDARD LUCENE              SCRIPTUM                          │
│   ──────────────               ────────                          │
│   segments_1  ← DELETED        segments_1  ✓ retained            │
│   segments_2  ← DELETED        segments_2  ✓ retained            │
│   segments_3  ← current        segments_3  ✓ retained            │
│                                segments_4  ✓ current             │
│                                                                  │
│   Only latest survives         All survive until GC              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation

```java
@Override
public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    // If no GC requested, keep everything
    if (gcCutoff == null) {
        commitSnapshot = new ArrayList<>(commits);  // Keep all!
        return;
    }

    // GC: delete old commits that don't have protected files
    for (IndexCommit commit : commits) {
        if (isBeforeCutoff(commit) && !hasProtectedFiles(commit)) {
            commit.delete();
        }
    }
}
```

### Commit Metadata

Each commit stores metadata for time-travel and history:

```java
// Stored in commit user-data
{
  "scriptum.uuid":       "550e8400-e29b-41d4-a716-446655440000",
  "scriptum.timestamp":  "2024-01-15T10:30:00Z",
  "scriptum.message":    "Added product catalog",
  "scriptum.branch":     "main",
  "scriptum.parent-ids": "previous-uuid,merge-source-uuid"
}
```

This enables:
- **Time travel**: Open reader at any historical commit
- **GC by timestamp**: Delete commits older than X
- **Commit graph**: Track parent-child relationships for merge history

---

## BranchAwareMergePolicy: Protecting Shared Segments

Lucene merges small segments into larger ones for efficiency. But if we merge segments that are shared with branches, we break those branches.

### The Problem

```
BEFORE MERGE:
  Main:   [_0] [_1] [_2]
  Branch: [_0] [_1] [_2] [_10000]  ← shares _0, _1, _2 with main

AFTER NAIVE MERGE ON MAIN:
  Main:   [_3]                     ← _0, _1, _2 merged into _3
  Branch: [_0] [_1] [_2] [_10000]  ← BROKEN! _0, _1, _2 deleted!
```

### The Solution

`BranchAwareMergePolicy` wraps the standard merge policy and filters merge proposals:

```java
private MergeSpecification filterMerges(MergeSpecification spec) {
    MergeSpecification filtered = new MergeSpecification();

    for (OneMerge merge : spec.merges) {
        boolean hasNonShared = false;
        for (SegmentCommitInfo sci : merge.segments) {
            if (!sharedSegmentNames.contains(sci.info.name)) {
                hasNonShared = true;
                break;
            }
        }
        // Only allow merges that include at least one non-shared segment
        if (hasNonShared) {
            filtered.add(merge);
        }
        // Reject merges of all-shared segments
    }

    return filtered.merges.isEmpty() ? null : filtered;
}
```

### Rules

| Merge Proposal | Decision | Reason |
|---------------|----------|--------|
| [_0, _1] (both shared) | **REJECT** | Would break branches |
| [_0, _10000] (mixed) | **ALLOW** | Creates new segment, preserves _0 |
| [_10000, _10001] (both local) | **ALLOW** | No sharing concerns |

---

## The Fork Operation

Forking creates a new branch in ~3-5ms regardless of index size:

```
┌─────────────────────────────────────────────────────────────────┐
│                    FORK OPERATION                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  STEP 1: Flush & commit current state                           │
│          (ensures all data is in segments)                       │
│                                                                  │
│  STEP 2: Read current SegmentInfos                              │
│          segments_5: [_0, _1, _2]                                │
│                                                                  │
│  STEP 3: Create overlay directory                               │
│          basePath/branches/experiment/                           │
│                                                                  │
│  STEP 4: Clone SegmentInfos with bumped counter                 │
│          cloned.counter = original.counter + 10000              │
│          (prevents segment name collisions)                      │
│                                                                  │
│  STEP 5: Write cloned manifest to branch                        │
│          branches/experiment/segments_1                          │
│                                                                  │
│  STEP 6: Track shared segments in merge policy                  │
│          sharedSegmentNames = {_0, _1, _2}                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Why Counter Bumping?

Lucene names segments sequentially: `_0`, `_1`, `_2`, etc. After forking:

```
Main (counter=3):      Next segment → _3
Branch (counter=10003): Next segment → _10003
```

The +10000 gap ensures branches never create segments with the same names as main, avoiding file conflicts in the base directory.

### Code

```java
public synchronized BranchIndexWriter fork(String newBranchName) throws IOException {
    // 1. Flush and commit
    writer.flush();
    writer.commit();

    // 2. Read current state
    SegmentInfos currentInfos = SegmentInfos.readLatestCommit(directory);

    // 3. Create overlay
    Path newOverlayPath = basePath.resolve("branches").resolve(newBranchName);
    Files.createDirectories(newOverlayPath);

    // 4. Clone with bumped counter
    SegmentInfos cloned = currentInfos.clone();
    cloned.counter = currentInfos.counter + 10000;

    // 5. Write to branch
    BranchedDirectory newBranchDir = new BranchedDirectory(baseDir, overlayDir, newBranchName);
    cloned.commit(newBranchDir);

    // 6. Track shared segments
    Set<String> sharedNames = new HashSet<>();
    for (SegmentCommitInfo sci : currentInfos) {
        sharedNames.add(sci.info.name);
    }
    mergePolicy.setSharedSegmentNames(sharedNames);

    // 7. Return new writer
    return new BranchIndexWriter(...);
}
```

### Cost Analysis

| Operation | Cost | Notes |
|-----------|------|-------|
| Flush | O(buffered docs) | Usually small |
| Read SegmentInfos | O(1) | Few KB file |
| Create directory | O(1) | Filesystem op |
| Clone SegmentInfos | O(segments) | Typically <100 segments |
| Write manifest | O(1) | Few KB |
| **Total** | **~3-5ms** | Independent of index size! |

---

## Garbage Collection

Without GC, segment files accumulate forever. Scriptum provides explicit GC with branch protection:

```
┌─────────────────────────────────────────────────────────────────┐
│                    GC FLOW                                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Scan all branches for referenced files                      │
│     branches/exp1/segments_* → {_0.cfs, _1.cfs, _5.cfs}        │
│     branches/exp2/segments_* → {_0.cfs, _2.cfs, _6.cfs}        │
│     protectedFiles = {_0.cfs, _1.cfs, _2.cfs, _5.cfs, _6.cfs}  │
│                                                                  │
│  2. Set GC cutoff on deletion policy                            │
│     deletionPolicy.setGcCutoff(before, protectedFiles)          │
│                                                                  │
│  3. Trigger commit (invokes onCommit callback)                  │
│     For each old commit:                                         │
│       if (timestamp < cutoff && !hasProtectedFiles):            │
│         commit.delete()  ← marks segment files for deletion     │
│                                                                  │
│  4. Lucene deletes unreferenced segment files                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Protected Files

A commit is protected if ANY of its files are referenced by a branch:

```java
private boolean hasProtectedFiles(IndexCommit commit, Set<String> protectedFiles) {
    for (String file : commit.getFileNames()) {
        if (protectedFiles.contains(file)) {
            return true;  // Keep this commit!
        }
    }
    return false;  // Safe to delete
}
```

### Branch Discovery

GC scans all branch directories to find referenced files:

```java
private Set<String> collectBranchReferencedFiles(Instant before) throws IOException {
    Set<String> protectedFiles = new HashSet<>();

    for (Path branchPath : Files.newDirectoryStream(branchesDir)) {
        for (String segmentsFile : overlayDir.listAll()) {
            if (!segmentsFile.startsWith("segments_")) continue;

            SegmentInfos infos = SegmentInfos.readCommit(branchDir, segmentsFile);

            // Only protect commits within grace period
            if (commitTime.isAfter(before)) {
                for (SegmentCommitInfo sci : infos) {
                    protectedFiles.addAll(sci.files());
                }
            }
        }
    }

    return protectedFiles;
}
```

---

## Performance Characteristics

### Space Complexity

| Component | Size |
|-----------|------|
| Base segments | O(total indexed data) |
| Branch overlay | O(branch-specific changes only) |
| Commit manifests | O(segments × commits) - typically KB each |

### Time Complexity

| Operation | Complexity | Typical Time |
|-----------|------------|--------------|
| Fork | O(segments) | 3-5ms |
| Add document | O(1) amortized | <1ms |
| Commit | O(buffered docs) | 1-50ms |
| Search | O(log n × segments) | <10ms |
| Open reader at generation | O(commits) | 1-5ms |
| GC | O(branches × commits) | 10-100ms |

### Memory

Branch writers share base segment data via memory-mapped files. Each branch only adds memory for:
- Overlay file tracking sets (~KB)
- Deleted base files set (~KB)
- Segment counter (8 bytes)

---

## Integration with Yggdrasil

Scriptum implements the [Yggdrasil](https://github.com/replikativ/yggdrasil) protocol stack, mapping Lucene concepts to Yggdrasil protocols:

| Yggdrasil Concept | Lucene Implementation |
|-------------------|----------------------|
| Snapshot ID | Commit UUID (stored in user-data) |
| Parent IDs | `scriptum.parent-ids` in commit user-data |
| Branch | Directory overlay (basePath/branches/name/) |
| Checkout | Open BranchIndexWriter on different branch |
| Merge | `addIndexes()` + parent-id tracking |

This enables Scriptum to participate in the Yggdrasil ecosystem alongside other COW data structures like Proximum.

---

## Summary

Scriptum extends Lucene with four key mechanisms:

1. **BranchedDirectory**: Overlay pattern separates branch writes from shared base
2. **BranchDeletionPolicy**: Retains all commits for time-travel and branch safety
3. **BranchAwareMergePolicy**: Prevents merging shared segments
4. **Counter bumping**: Avoids segment name collisions between branches

The result: Git-like branching on full-text search indices with O(segments) fork cost, regardless of index size.
