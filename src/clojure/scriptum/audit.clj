(ns scriptum.audit
  "Audit-chain verification for `:crypto-hash? true` scriptum branches.

   `verify-chain` walks the linear sequence of Lucene commit generations
   for a branch, asks the underlying `BranchIndexWriter.verifyCommit` to
   recompute each commit's segment-file hashes against the stored
   merkle metadata, and reports any divergence.

   The protocol shape (`IAuditable`, `-merkle-root`,
   `-recompute-merkle-root`) and the result-map vocabulary
   (`{:status :ok|:mismatch|:unsupported|:advisory|:incomplete}`) are
   intentionally identical to the rest of the replikativ index libs
   (datahike.index.audit, stratum.audit, proximum.audit). Bridges in
   datahike pass results through without translation."
  (:require [scriptum.core :as sc])
  (:import [org.apache.lucene.index DirectoryReader IndexCommit]
           [org.replikativ.scriptum BranchIndexWriter]
           [java.util UUID]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Protocol — same shape as datahike.index.audit/IAuditable
;; ============================================================================

(defprotocol IAuditable
  (-merkle-root [this]
    "Cheap. Returns the cached/known content-addressed UUID of this
     thing's current state. Returns nil when no commit has happened yet
     or `:crypto-hash?` was off. Never throws.")

  (-recompute-merkle-root [this]
    "Expensive. Re-derives the merkle root by re-hashing the underlying
     storage and confirming it matches the cached root. Returns a
     result map; never throws on mismatch:

       {:status :ok          :root <uuid>}
       {:status :mismatch    :root <recomputed?>
                             :errors [{:address, :expected, :recomputed,
                                       :details}]}
       {:status :unsupported :reason <kw>}"))

;; ============================================================================
;; Single-commit verification — wraps sc/verify-commit into the result map
;; ============================================================================

(defn- coerce-uuid [s]
  (when s (UUID/fromString ^String s)))

(defn- verify-one
  "Verify one commit (-1 = HEAD). Translates scriptum's
   `{:valid? :commit-id :errors}` into the unified result map. For HEAD
   verification, `:root` carries the content-hash (merkle root) so it
   matches what `-merkle-root` returns; for older generations the per-
   commit content-hash isn't exposed by `verifyCommit`, so `:root`
   carries the Lucene commit-id instead."
  [writer ^Long generation]
  (let [^BranchIndexWriter bw (sc/->writer writer)
        r (try
            (if (neg? generation)
              (sc/verify-commit writer)
              (sc/verify-commit writer {:generation generation}))
            (catch IllegalStateException _
              {:valid? false :crypto-disabled? true}))
        commit-uuid (coerce-uuid (:commit-id r))
        head?       (neg? generation)
        head-root   (when head?
                      (some-> (.getLastContentHash bw) UUID/fromString))]
    (cond
      (:crypto-disabled? r)
      {:status :unsupported :reason :crypto-hash-disabled}

      (:valid? r)
      {:status :ok :root (or head-root commit-uuid)}

      :else
      {:status :mismatch :root nil
       :errors [{:type :audit/merkle-mismatch
                 :address (or head-root commit-uuid)
                 :expected (or head-root commit-uuid)
                 :details (vec (:errors r))}]})))

;; ============================================================================
;; Chain walk
;; ============================================================================

(defn- list-generations
  "Return the sequence of Lucene commit generations on this writer's
   branch, oldest first. Each generation is a single integer."
  [^BranchIndexWriter bw]
  (let [commits (DirectoryReader/listCommits (.getDirectory bw))]
    (->> commits
         (map (fn [^IndexCommit c]
                {:generation (.getGeneration c)
                 :segments-file (.getSegmentsFileName c)}))
         (sort-by :generation))))

(defn verify-chain
  "Walk every Lucene commit on `writer`'s branch, recompute each
   commit's segment-file hashes via `verifyCommit`, and return

     {:head <head-commit-uuid>
      :status :ok | :mismatch | :advisory | :incomplete
      :commits [{:generation, :cid, :status [, :errors :reason]}]
      :mismatches [...]
      :missing []}

   Options:
     :from-gen  — start at this generation (default: oldest)
     :to-gen    — stop at this generation (default: newest)
     :limit     — max commits to verify (default: unbounded)

   Without `:crypto-hash?` enabled, every entry is reported as
   `:unsupported :reason :crypto-hash-disabled` and the overall
   `:status` is `:advisory`."
  ([writer] (verify-chain writer {}))
  ([writer {:keys [from-gen to-gen limit]
            :or {limit Long/MAX_VALUE}}]
   (let [^BranchIndexWriter bw (sc/->writer writer)
         all (list-generations bw)
         filtered (cond->> all
                    from-gen (filter #(>= (:generation %) from-gen))
                    to-gen   (filter #(<= (:generation %) to-gen))
                    true     (take limit))
         entries (mapv
                  (fn [{:keys [generation]}]
                    (let [r (verify-one writer (long generation))]
                      (cond-> {:generation generation
                               :status (:status r)}
                        (:root r)    (assoc :cid (:root r))
                        (:errors r)  (assoc :errors (:errors r))
                        (:reason r)  (assoc :reason (:reason r)))))
                  filtered)
         mism (filterv #(= :mismatch (:status %)) entries)
         adv  (some #(= :unsupported (:status %)) entries)
         status (cond (seq mism) :mismatch
                      adv        :advisory
                      :else      :ok)
         head-cid (some-> entries last :cid)]
     {:head head-cid
      :status status
      :commits entries
      :mismatches mism
      :missing []})))

(defn ok? [report] (= :ok (:status report)))

;; ============================================================================
;; Live-instance protocol extension — bridges (e.g. datahike's scriptum
;; secondary) hold a live ScriptumWriter and call -recompute-merkle-root
;; to verify the current HEAD without iterating the whole chain.
;; ============================================================================

(extend-protocol IAuditable
  scriptum.core.ScriptumWriter
  (-merkle-root [w]
    (let [^BranchIndexWriter bw (sc/->writer w)
          h (.getLastContentHash bw)]
      (when h (UUID/fromString h))))
  (-recompute-merkle-root [w]
    (verify-one w -1)))
