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
    (assoc-in store [name :data] (into (if (contains? store name)
      (:data (store name))
      (sorted-set)) statement))
    (throw (ex-info "Invalid statement." (s/explain ::l/statements statement)))))

(defn- remove-statement-impl
  [store name statement]
  (if (s/valid? ::l/statement statement)
    (assoc-in store [name :data] (set/difference (if (contains? store name)
      (:data (store name))
      (sorted-set)) statement))
    (throw (ex-info "Invalid statement." (s/explain ::l/statements statement)))))

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

(defn- match-statements-impl
  [store name pattern]
  (if-let [collection (get-in store [name :data])]
    (filter (comment TODO) collection)
    #{}))

(defn- add-rule-impl
  [store name rule]
  (comment TODO 2))

(defn- remove-rule-impl
  [store name rule]
  (comment TODO 2))

(defn- all-rules-impl
  [store name]
  (comment TODO 2))

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
  [store]
  (reify l/ReadTx
    (all-statements
      [this]
      (all-statements-impl @store name))
    (l/match-statements
      [this pattern]
      (swap! store #(match-statements-impl % name pattern)))
    (all-rules
      [this]
      (swap! store #(all-rules-impl % name)))
    (match-rules
      [this pattern]
      (swap! store #(match-rules-impl % name pattern)))
    (sparql-query
      [this query]
      (swap! store #(sparql-query-impl % name query)))
    (wander-query
      [this query]
      (swap! store #(wander-query-impl % name query)))))

(defn- ligature-write-tx
  "Create a read/write transaction for Ligature."
  [store]
  (reify l/ReadTx l/WriteTx
    (all-statements
      [this]
      (all-statements-impl @store name))
    (l/match-statements
      [this pattern]
      (swap! store #(match-statements-impl % name pattern)))
    (add-statement
      [this statement]
      (swap! store #(add-statement-impl % name statement)))
    (remove-statement
      [this statement]
      (swap! store #(remove-statement-impl % name statement)))
    (new-identifier
      [this]
      (str "_:" (get-in (swap! store #(new-identifier-impl % name)) [name :id-counter])))
    (add-rule
      [this rule]
      (swap! store #(add-rule-impl % name rule)))
    (remove-rule
      [this rule]
      (swap! store #(remove-rule-impl % name rule)))
    (all-rules
      [this]
      (swap! store #(all-rules-impl % name)))
    (match-rules
      [this pattern]
      (swap! store #(match-rules-impl % name pattern)))
    (sparql-query
      [this query]
      (swap! store #(sparql-query-impl % name query)))
    (wander-query
      [this query]
      (swap! store #(wander-query-impl % name query)))))

(defn- ligature-memory-collection
  "Creates an in-memory implementation of the LigatureCollection protocol."
  [store name]
  (reify l/LigatureCollection
    (collection-name
      [this]
      name)
    (readTx
      [this]
      (ligature-read-tx store))
    (writeTx
      [this]
      (ligature-write-tx store))))

(defn ligature-memory-store
  "Creates an in-memory implementation of the LigatureStore protocol."
  []
  (let [store (atom {})]
    (reify l/LigatureStore
      (collection
        [this collection-name]
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
