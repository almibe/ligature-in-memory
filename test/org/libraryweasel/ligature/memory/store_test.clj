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
  (testing "Basic empty store functionality."
    (let [store (ligature-memory-store)]
      (is (not (= (collection store "test") nil)))
      (is (= (all-collections store) #{}))
      (is (not (= (collection store "test2") nil)))
      (is (= (all-collections store) #{}))
      (delete-collection store "test")
      (is (= (all-collections store) #{}))
      (testing "Basic collection functionality."
        (let [collection (collection store "test")]
          (is (= (collection-name collection) "test"))
          (is (= (set (all-statements collection)) #{}))
          (add-statements collection [[ "This" :a "test"]])
          (is (= (set (all-statements collection)) #{[ "This" :a "test"]}))
          (add-statements collection [[ "a" :a "a"] [ "b" :a "b"]])
          (add-statements collection [[ "c" :a "c" "c"]])
          (is (= (set (all-statements collection)) #{[ "This" :a "test"]
                                                  [ "a" :a "a"]
                                                  [ "b" :a "b"]
                                                  [ "c" :a "c" "c"]}))
          (remove-statements collection [])
          (is (= (set (all-statements collection)) #{[ "This" :a "test"]
                                                     [ "a" :a "a"]
                                                     [ "b" :a "b"]
                                                     [ "c" :a "c" "c"]}))
          (remove-statements collection [["a" :a "a"] ["b" :a "b"] ["d" :a "d"]])
          (is (= (set (all-statements collection)) #{[ "This" :a "test"]
                                                     [ "c" :a "c" "c"]})))))))
