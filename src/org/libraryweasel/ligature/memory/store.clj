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

(defn- add-statements-impl
  [store name statements]
  (if (s/valid? ::l/statements statements)
    (conj (if (contains? store name)
      (store name)
      (assoc store name (sorted-set))) statements)
    (throw (ex-info "Invalid statement." {}))))

(defn- remove-statements-impl
  [store name statements]
  (if (s/valid? ::l/statements statements)
    (set/difference (if (contains? store name)
      (store name)
      (assoc store name (sorted-set))) statements)
    (throw (ex-info "Invalid statement." {}))))

(defn- new-identifier-impl
  [store name]
  (comment TODO 1))

(defn- match-statements-impl
  [store name pattern]
  (comment TODO 1))

(defn- add-rules-impl
  [store name rules]
  (comment TODO 2))

(defn- remove-rules-impl
  [store name rules]
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

(defn- ligature-memory-collection
  "Creates an in-memory implementation of the LigatureCollection protocol."
  [store name]
  (reify l/LigatureCollection
    (add-statements
      [this statements]
      (swap! store #(add-statements-impl % name statements)))
    (remove-statements
      [this statements]
      (swap! store #(remove-statements-impl % name statements)))
    (all-statements
      [this]
      (keys (:data @store)))
    (new-identifier
      [this]
      (swap! store #(new-identifier-impl % name)))
    (l/match-statements
      [this pattern]
      (swap! store #(match-statements-impl % name pattern)))
    (collection-name
      [this]
      name)
    (add-rules
      [this rules]
      (swap! store #(add-rules-impl % name rules)))
    (remove-rules
      [this rules]
      (swap! store #(remove-rules-impl % name rules)))
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
        (swap! store (do
                       #(dissoc (:rules %) collection-name)
                       #(dissoc (:data %) collection-name))))
      (all-collections
        [this]
        (set (keys (:data @store))))
      (close
        [this]
        (swap! store {}))
      (details
        [this]
        {:location "memory"}))))
