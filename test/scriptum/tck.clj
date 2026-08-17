(ns scriptum.tck
  "Bridge for Lucene's own `Directory` conformance suite.

  `BaseDirectoryTestCase` is the contract every Lucene `Directory` is held to —
  it is what `MMapDirectory` and `ByteBuffersDirectory` are tested with, and it
  exercises corners hand-written tests reliably miss: concurrent readers over a
  file being written, `IndexInput` clones and slices past EOF, the exact
  exception types for a missing/duplicate file, use-after-close, and the
  interaction of `listAll` with pending deletions.

  Running it against `scriptum.konserve` is the cheapest way to find out whether
  a store-backed Directory really is a Directory. The suite is a JUnit class, so
  the Java side (`ScriptumDirectoryTCK`) drives it and calls back in here for the
  one thing it cannot construct itself."
  (:require [konserve.store :as kstore]
            [scriptum.konserve :as sk]))

(def ^:private store-ids (atom {}))

(defn directory-for
  "A konserve-backed `Directory` rooted at `path`, for one TCK run.

  The suite hands out a fresh temp path per test and expects an independent
  Directory over it, so store and cache both live under that path."
  ^org.apache.lucene.store.Directory [^String path]
  (let [sp (str path "/store")
        ;; A constant random UUID per store, memoized by path: konserve's `:id`
        ;; is a global address and must not be derived from the location. The
        ;; suite hands out a fresh path per test, so this mints one per store
        ;; and reuses it if the same path is opened again.
        id (get (swap! store-ids update sp #(or % (random-uuid))) sp)
        store (kstore/create-store {:backend :file :path sp :id id} {:sync? true})]
    (sk/konserve-directory store (str path "/cache") "main")))
