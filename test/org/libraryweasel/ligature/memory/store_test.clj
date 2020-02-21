; Copyright (c) 2019-2020 Alex Michael Berry
;
; This program and the accompanying materials are made
; available under the terms of the Eclipse Public License 2.0
; which is available at https://www.eclipse.org/legal/epl-2.0/
;
; SPDX-License-Identifier: EPL-2.0

(ns org.libraryweasel.ligature.memory.store-test
  (:require [clojure.test :refer :all]
            [org.libraryweasel.ligature.memory.store :refer :all]
            [org.libraryweasel.ligature.core :refer :all]))

(deftest store-test
  (testing "Create and close store"
    (let [store (ligature-memory-store)]
      (is (= (details store) {:location "memory"}))
      (is (= (all-collections store) #{}))
      (close store)))

  (testing "access new collection"
    (let [store (ligature-memory-store)]
      (is (not (= (collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (is (= (all-collections store) #{}))))

  (testing "creating a new collection"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (is (= (all-collections store) #{"test"}))))
  
  (testing "access and delete new collection"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (is (= (all-collections store) #{"test"}))
      (delete-collection store "test")
      (delete-collection store "test2")
      (is (= (all-collections store) #{}))))

  (testing "new collections should be empty"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (readTx (collection store "test"))]
        (is (= (set (all-statements tx)) #{}))
        (is (= (set (all-rules tx)) #{}))
        (cancel tx))))

  (testing "adding statements to collections"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (writeTx (collection store "test"))]
        (add-statement tx ["This" a "test"])
        (commit tx))
      (let [tx (readTx (collection store "test"))]
        (is (= (set (all-statements tx)) #{["This" a "test"]}))
        (cancel tx))))

  (testing "adding rules to collections"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (writeTx (collection store "test"))]
        (add-rule tx ["Also" a "test"])
        (commit tx))
      (let [tx (readTx (collection store "test"))]
        (is (= (set (all-rules tx)) #{["Also" a "test"]}))
        (cancel tx))))

  (testing "removing statements from collections"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (writeTx (collection store "test"))]
        (add-statement tx ["This" a "test"])
        (add-statement tx ["Also" a "test"])
        (remove-statement tx ["This" a "test"])
        (commit tx))
      (let [tx (readTx (collection store "test"))]
        (is (= (set (all-statements tx)) #{["Also" a "test"]}))
        (cancel tx))))

  (testing "removing rules from collections"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (writeTx (collection store "test"))]
        (add-rule tx ["This" a "test"])
        (add-rule tx ["Also" a "test"])
        (remove-rule tx ["This" a "test"])
        (commit tx))
      (let [tx (readTx (collection store "test"))]
        (is (= (set (all-rules tx)) #{["Also" a "test"]}))
        (cancel tx))))

  (testing "matching statements in collections"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (writeTx (collection store "test"))]
        (add-statement tx ["This" a "test"])
        (add-statement tx [(new-identifier tx) a "test"])
        (add-statement tx ["a" "knows" "b"])
        (add-statement tx ["b" "knows" "c"])
        (add-statement tx ["c" "knows" "a"])
        (add-statement tx ["c" "knows" "a"]) ; dupe
        (add-statement tx [(new-identifier tx) (new-identifier tx) (new-identifier tx) (new-identifier tx)])
        (commit tx))
      (let [tx (readTx (collection store "test"))]
        (is (= (count (match-statements tx [:? :? :?])) 6))
        (is (= (set (match-statements tx [:? a :?])) #{["This" a "test"] ["Also" a "test"]}))
        (is (= (set (match-statements tx [:? a "test"])) #{["This" a "test"] ["Also" a "test"]}))
        (is (= (set (match-statements tx [:? :? "test"])) #{["This" a "test"] ["Also" a "test"]}))
        (is (= (set (match-statements tx [:? :? :? ":_5"])) #{["_:2" "_:3" "_:4" "_:5"]}))
        (cancel tx))))

  (testing "matching rules in collections"
    (let [store (ligature-memory-store)]
      (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (let [tx (writeTx (collection store "test"))]
        (add-rule tx ["This" a "test"])
        (add-rule tx [(new-identifier tx) a "test"])
        (add-rule tx ["a" "knows" "b"])
        (add-rule tx ["b" "knows" "c"])
        (add-rule tx ["c" "knows" "a"])
        (add-rule tx ["c" "knows" "a"]) ; dupe
        (add-rule tx [(new-identifier tx) (new-identifier tx) (new-identifier tx) (new-identifier tx)])
        (commit tx))
      (let [tx (readTx (collection store "test"))]
        (is (= (count (match-rules tx [:? :? :?])) 6))
        (is (= (set (match-rules tx [:? a :?])) #{["This" a "test"] ["Also" a "test"]}))
        (is (= (set (match-rules tx [:? a "test"])) #{["This" a "test"] ["Also" a "test"]}))
        (is (= (set (match-rules tx [:? :? "test"])) #{["This" a "test"] ["Also" a "test"]}))
        (is (= (set (match-rules tx [:? :? :? ":_5"])) #{["_:2" "_:3" "_:4" "_:5"]}))
        (cancel tx)))))
