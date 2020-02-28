; Copyright (c) 2019-2020 Alex Michael Berry
;
; This program and the accompanying materials are made
; available under the terms of the Eclipse Public License 2.0
; which is available at https://www.eclipse.org/legal/epl-2.0/
;
; SPDX-License-Identifier: EPL-2.0

(ns org.libraryweasel.ligature.memory.store
  (:require [org.libraryweasel.ligature.core :as l])
  (:require [clojure.spec.alpha :as s])
  (:require [clojure.set :as set]))

; TODO possibly rewrite strategy
;; have a single swap in the first line of the protocol impl function where it is needed
;; and then perform all functionality in private functions that don't directly call swap ever

(defn- add-statement-impl
  [store name statement]
  (if (s/valid? ::l/statement statement)
    (assoc-in store [name :data] (conj (if (and (contains? store name) (contains? (store name) :data))
      (:data (store name))
      (sorted-set)) statement))
    (throw (ex-info "Invalid statement." (s/explain-data ::l/statement statement)))))

(defn- remove-statement-impl
  [store name statement]
  (if (s/valid? ::l/statement statement)
    (assoc-in store [name :data] (disj (if (contains? store name)
      (:data (store name))
      (sorted-set)) statement))
    (throw (ex-info "Invalid statement." (s/explain-data ::l/statement statement)))))

(defn- all-statements-impl
  [store name]
  (if (contains? store name)
    (:data (store name))
    (sorted-set)))

(defn- first-new-identifier
  [store name]
  (assoc-in store [name :id-counter] 1))

(defn- new-identifier-impl
  [store name]
  (if-let [id-counter (get-in store [name :id-counter])]
    (assoc-in store [name :id-counter] (inc id-counter))
    (first-new-identifier store name)))

(defn- match
  [pattern-el data-el]
  (if (= pattern-el :?)
    true
    (= pattern-el data-el)))

(defn- match-filter
  [pattern]
  #(and (match (l/subject pattern) (l/subject %))
        (match (l/predicate pattern) (l/predicate %))
        (match (l/object pattern) (l/object %))
        (match (l/graph pattern) (l/graph %))))

(defn- match-statements-impl
  [store name pattern]
  (if-let [collection (get-in store [name :data])]
    (filter (match-filter pattern) collection)
    ;(println (str "match - " collection))
    #{}))

(defn- add-rule-impl
  [store name rule]
  (if (s/valid? ::l/rule rule)
    (assoc-in store [name :rules] (conj (if (and (contains? store name) (contains? (store name) :rules))
      (:rules (store name))
      (sorted-set)) rule))
    (throw (ex-info "Invalid rule." (s/explain-data ::l/rule rule)))))

(defn- remove-rule-impl
  [store name rule]
  (if (s/valid? ::l/rule rule)
    (assoc-in store [name :rules] (disj (if (contains? store name)
      (:rules (store name))
      (sorted-set)) rule))
    (throw (ex-info "Invalid rule." (s/explain-data ::l/rule rule)))))

(defn- all-rules-impl
  [store name]
  (if (contains? store name)
    (:rules (store name))
    (sorted-set)))

(defn- match-rules-impl
  [store name pattern]
  (comment TODO 2))

(defn- sparql-query-impl
  [store name query]
  (comment TODO 3))

(defn- wander-query-impl
  [store name query]
  (comment TODO 3))

(defn- ligature-read-tx
  "Create a read-only transaction for Ligature."
  [store name]
  (reify l/ReadTx
    (l/all-statements
      [this]
      (all-statements-impl @store name))
    (l/match-statements
      [this pattern]
      (match-statements-impl @store name pattern))
    (l/all-rules
      [this]
      (all-rules-impl @store name))
    (l/match-rules
      [this pattern]
      (swap! store #(match-rules-impl % name pattern)))
    (l/sparql-query
      [this query]
      (swap! store #(sparql-query-impl % name query)))
    (l/wander-query
      [this query]
      (swap! store #(wander-query-impl % name query)))
    (l/cancel
      [this]
      (comment do nothing))))

(defn- ligature-write-tx
  "Create a read/write transaction for Ligature."
  [store name]
  (reify l/ReadTx l/WriteTx
    (l/all-statements
      [this]
      (all-statements-impl @store name))
    (l/match-statements
      [this pattern]
      (swap! store #(match-statements-impl % name pattern)))
    (l/add-statement
      [this statement]
      (swap! store #(add-statement-impl % name statement)))
    (l/remove-statement
      [this statement]
      (swap! store #(remove-statement-impl % name statement)))
    (l/new-identifier
      [this]
      (str "_:" (get-in (swap! store #(new-identifier-impl % name)) [name :id-counter])))
    (l/add-rule
      [this rule]
      (swap! store #(add-rule-impl % name rule)))
    (l/remove-rule
      [this rule]
      (swap! store #(remove-rule-impl % name rule)))
    (l/all-rules
      [this]
      (swap! store #(all-rules-impl % name)))
    (l/match-rules
      [this pattern]
      (swap! store #(match-rules-impl % name pattern)))
    (l/sparql-query
      [this query]
      (swap! store #(sparql-query-impl % name query)))
    (l/wander-query
      [this query]
      (swap! store #(wander-query-impl % name query)))
    (l/commit 
      [this]
      (comment TODO))
    (l/cancel
      [this]
      (comment TODO))))

(defn- ligature-memory-collection
  "Creates an in-memory implementation of the LigatureCollection protocol."
  [store name]
  (reify l/LigatureCollection
    (collection-name
      [this]
      name)
    (readTx
      [this]
      (ligature-read-tx store name))
    (writeTx
      [this]
      (ligature-write-tx store name))))

(defn ligature-memory-store
  "Creates an in-memory implementation of the LigatureStore protocol."
  []
  (let [store (atom {})]
    (reify l/LigatureStore
      (collection
        [this collection-name]
        (ligature-memory-collection store collection-name))
      (create-collection
        [this collection-name]
        (if (contains? @store name)
          (comment do nothing)
          (swap! store #(assoc % collection-name {})))
        (ligature-memory-collection store collection-name))
      (delete-collection
        [this collection-name]
        (swap! store #(dissoc % collection-name)))
      (all-collections
        [this]
        (set (keys @store)))
      (close
        [this]
        (swap! store {}))
      (details
        [this]
        {:location "memory"}))))
