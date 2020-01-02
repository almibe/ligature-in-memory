; Copyright (c) 2019-2020 Alex Michael Berry
;
; This program and the accompanying materials are made
; available under the terms of the Eclipse Public License 2.0
; which is available at https://www.eclipse.org/legal/epl-2.0/
;
; SPDX-License-Identifier: EPL-2.0

(ns org.libraryweasel.ligature.memory.store
  (:require [org.libraryweasel.ligature.core :refer :all]))

(defn ligature-memory-store
  "Creates an in-memory implementation of the LigatureStore protocol."
  []
  (let [datasets (atom {})]
    (reify LigatureStore
      (get-dataset
        [this dateset-name]
        ((swap! datasets
          #(when (not (contains? % dateset-name))
            (conj % [dateset-name {}]))) dateset-name))
      (delete-dataset
        [this dataset-name]
        (swap! datasets #(dissoc % dataset-name)))
      (all-datasets
        [this]
        (set (keys @datasets)))
      (close
        [this]
        (comment "do nothing"))
      (location
        [this]
        "memory"))))

(defn- ligature-memory-dataset
  "Creates an in-memory implementation of the LigatureDataset protocol."
  [store name]
  (reify LigatureDataset
    (add-statements
      [this statements]
      (comment TODO))
    (remove-statements
      [this statements]
      (comment TODO))
    (all-statements
      [this]
      (comment TODO))
    (new-identifier
      [this]
      (comment TODO))
    (match-statements
      [this pattern]
      (comment TODO))
    (dataset-name
      [this]
      (comment TODO))
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
