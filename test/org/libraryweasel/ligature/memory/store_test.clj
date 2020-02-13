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
      (is (= (all-collections store) #{"test"}))))

  (testing "access and delete new collection"
    (let [store (ligature-memory-store)]
      (is (not (= (collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
      (is (= (all-collections store) #{"test"}))
      (delete-collection store "test")
      (delete-collection store "test2")
      (is (= (all-collections store) #{}))))

  (testing "new collections should be empty")

  (testing "adding statements to collections")

  (testing "removing statements from collections")

  (testing "matching statements in collections")

  (testing "adding rules to collections")

  (testing "removing rules from collections")

  (testing "matching rules in collections"))
  
  ; (testing "Basic empty store functionality."
  ;   (let [store (ligature-memory-store)]
  ;     (testing "Basic collection functionality."
  ;       (let [collection (collection store "test")]
  ;         (is (= (collection-name collection) "test"))
  ;         (is (= (set (all-statements collection)) #{}))
  ;         (add-statements collection [["This" :a "test"]])
  ;         (is (= (all-statements collection) #{["This" :a "test"]}))
  ;         (add-statements collection [["a" :a "a"] ["b" :a "b"]])
  ;         (add-statements collection [["c" :a "c" "c"]])
  ;         (is (= (set (all-statements collection)) #{["This" :a "test"]
  ;                                                    ["a" :a "a"]
  ;                                                    ["b" :a "b"]
  ;                                                    ["c" :a "c" "c"]}))
  ;         (remove-statements collection [])
  ;         (is (= (set (all-statements collection)) #{["This" :a "test"]
  ;                                                    ["a" :a "a"]
  ;                                                    ["b" :a "b"]
  ;                                                    ["c" :a "c" "c"]}))
  ;         (remove-statements collection [["a" :a "a"] ["b" :a "b"] ["d" :a "d"]])
  ;         (is (= (set (all-statements collection)) #{["This" :a "test"]
  ;                                                    ["c" :a "c" "c"]}))
  ;         (add-statements collection [[(new-identifier collection)
  ;                                      "knows"
  ;                                      (new-identifier collection)
  ;                                      (new-identifier collection)]])
  ;         (is (= (set (all-statements collection)) #{["This" :a "test"]
  ;                                                    ["_:1" "knows" "_:2" "_:3"]
  ;                                                    ["c" :a "c" "c"]}))
  ;         (is (= (set (match-statements collection [:? :? :? :?])) #{["This" :a "test"]
  ;                                                                    ["_:1" "knows" "_:2" "_:3"]
  ;                                                                    ["c" :a "c" "c"]}))
  ;         (is (= (set (match-statements collection [:? :? :?])) #{["This" :a "test"]}))
  ;         (is (= (set (match-statements collection [:? :a :? :?])) #{["This" :a "test"]
  ;                                                                    ["c" :a "c" "c"]}))
  ;         (is (= (set (match-statements collection ["c" :? "c" :?])) #{["c" :a "c" "c"]})))))))
