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
  (testing "Create new store."
    (is (not (= (ligature-memory-store) nil))))
  (testing "Basic store functionality."
    (let [store (ligature-memory-store)]
      (is (not (= (collection store "test") nil)))
      (is (= (all-collections store) #{"test"}))
      (is (not (= (collection store "test2") nil)))
      (is (= (all-collections store) #{"test" "test2"}))
      (delete-collection store "test")
      (is (= (all-collections store) #{"test2"}))
      (testing "Basic collection functionality."
        (let [collection (collection store "test")]
          (is (= (collection-name collection) "test"))
          (is (= (set (all-statements collection)) #{}))
          (add-statements collection [(statement "This" :a "test")])
          (is (= (set (all-statements collection)) #{(statement "This" :a "test")}))
          (add-statements collection [(statement "a" :a "a") (statement "b" :a "b")])
          (add-statements collection [(statement "c" :a "c" "c")])
          (is (= (set (all-statements collection)) #{(statement "This" :a "test")
                                                  (statement "a" :a "a")
                                                  (statement "b" :a "b")
                                                  (statement "c" :a "c" "c")})))))))
