/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */





 /*
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
          (add-statements collection [["This" :a "test"]])
          (is (= (all-statements collection) #{["This" :a "test"]}))
          (add-statements collection [["a" :a "a"] ["b" :a "b"]])
          (add-statements collection [["c" :a "c" "c"]])
          (is (= (set (all-statements collection)) #{["This" :a "test"]
                                                  ["a" :a "a"]
                                                  ["b" :a "b"]
                                                  ["c" :a "c" "c"]}))
          (remove-statements collection [])
          (is (= (set (all-statements collection)) #{["This" :a "test"]
                                                     ["a" :a "a"]
                                                     ["b" :a "b"]
                                                     ["c" :a "c" "c"]}))
          (remove-statements collection [["a" :a "a"] ["b" :a "b"] ["d" :a "d"]])
          (is (= (set (all-statements collection)) #{["This" :a "test"]
                                                     ["c" :a "c" "c"]}))
          (add-statements collection [[(new-identifier collection)
                                       "knows"
                                       (new-identifier collection)
                                       (new-identifier collection)]])
          (is (= (set (all-statements collection)) #{["This" :a "test"]
                                                     ["_:1" "knows" "_:2" "_:3"]
                                                     ["c" :a "c" "c"]}))
          (is (= (set (match-statements collection [:? :? :? :?])) #{["This" :a "test"]
                                                                     ["_:1" "knows" "_:2" "_:3"]
                                                                     ["c" :a "c" "c"]}))
          (is (= (set (match-statements collection [:? :? :?])) #{["This" :a "test"]}))
          (is (= (set (match-statements collection [:? :a :? :?])) #{["This" :a "test"]
                                                                     ["c" :a "c" "c"]}))
          (is (= (set (match-statements collection ["c" :? "c" :?])) #{["c" :a "c" "c"]})))))))

 */