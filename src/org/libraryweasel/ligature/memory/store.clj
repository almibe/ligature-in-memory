; Copyright (c) 2019-2020 Alex Michael Berry
;
; This program and the accompanying materials are made
; available under the terms of the Eclipse Public License 2.0
; which is available at https://www.eclipse.org/legal/epl-2.0/
;
; SPDX-License-Identifier: EPL-2.0

(ns org.libraryweasel.ligature.memory.store
  (:require [org.libraryweasel.ligature.core :refer :all]))

; TODO possibly rewrite strategy
;; have a single swap in the first line of the protocol impl function where it is needed
;; and then perform all functionality in private functions that don't directly call swap ever

(defn- add-statements-impl
  [store name statements]
  )

(defn- ligature-memory-collection
  "Creates an in-memory implementation of the LigatureCollection protocol."
  [store name]
  (reify LigatureCollection
    (add-statements
      [this statements]
      (swap! store #(add-statements-impl % name statements)))
    (remove-statements
      [this statements]
      (comment TODO))
    (all-statements
      [this]
      (keys (:data @store)))
    (new-identifier
      [this]
      (comment TODO))
    (match-statements
      [this pattern]
      (comment TODO))
    (collection-name
      [this]
      name)
    (add-rules
      [this rules]
      (comment TODO))
    (remove-rules
      [this rules]
      (comment TODO))
    (all-rules
      [this]
      (comment TODO))
    (match-rules
      [this pattern]
      (comment TODO))
    (sparql-query
      [this query]
      (comment TODO))
    (wander-query
      [this query]
      (comment TODO))))

(defn ligature-memory-store
  "Creates an in-memory implementation of the LigatureStore protocol."
  []
  (let [store (atom {:data {} :rules {}})]
    (reify LigatureStore
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
        (swap! store {:data {} :rules {}}))
      (details
        [this]
        {:location "memory"}))))
