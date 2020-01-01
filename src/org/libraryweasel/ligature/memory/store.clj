; Copyright (c) 2019 Alex Michael Berry
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
        [this dateset-name]
        (comment remove dataset-name))
      (all-datasets
        [this]
        (keys datasets))
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
      )
    (remove-statements
      [this statements]
      )
    (all-statements
      [this]
      )
    (new-identifier
      [this]
      )
    (match-statements
      [this pattern]
      )
    (dataset-name
      [this]
      )
    (add-rules
      [this rules]
      )
    (remove-rules
      [this rules]
      )
    (all-rules
      [this]
      )
    (match-rules
      [this pattern]
      )
    (sparql-query
      [this query]
      )
    (wander-query
      [this query]
      )))
