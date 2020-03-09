/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import io.kotlintest.specs.StringSpec

class InMemorySpec: StringSpec({
//    (ns org.libraryweasel.ligature.memory.store-test
//    (:require [clojure.test :refer :all]
//    [org.libraryweasel.ligature.memory.store :refer :all]
//    [org.libraryweasel.ligature.core :refer :all]))
//
//    (deftest store-test
//    (testing "Create and close store"
//    (let [store (ligature-memory-store)]
//    (is (= (details store) {:location "memory"}))
//    (is (= (all-collections store) #{}))
//    (close store)))
//
//    (testing "access new collection"
//    (let [store (ligature-memory-store)]
//    (is (not (= (collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (is (= (all-collections store) #{}))))
//
//    (testing "creating a new collection"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (is (= (all-collections store) #{"test"}))))
//
//    (testing "access and delete new collection"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (is (= (all-collections store) #{"test"}))
//    (delete-collection store "test")
//    (delete-collection store "test2")
//    (is (= (all-collections store) #{}))))
//
//    (testing "new collections should be empty"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (let [tx (readTx (collection store "test"))]
//    (is (= (set (all-statements tx)) #{}))
//    (is (= (set (all-rules tx)) #{}))
//    (cancel tx))))
//
//    (testing "adding statements to collections"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (let [tx (writeTx (collection store "test"))]
//    (add-statement tx ["This" a "test" _])
//    (commit tx))
//    (let [tx (readTx (collection store "test"))]
//    (is (= (set (all-statements tx)) #{["This" a "test" _]}))
//    (cancel tx))))
//
//    (testing "adding rules to collections"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (let [tx (writeTx (collection store "test"))]
//    (add-rule tx ["Also" a "test"])
//    (commit tx))
//    (let [tx (readTx (collection store "test"))]
//    (is (= (set (all-rules tx)) #{["Also" a "test"]}))
//    (cancel tx))))
//
//    (testing "removing statements from collections"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (let [tx (writeTx (collection store "test"))]
//    (add-statement tx ["This" a "test" _])
//    (add-statement tx ["Also" a "test" _])
//    (remove-statement tx ["This" a "test" _])
//    (commit tx))
//    (let [tx (readTx (collection store "test"))]
//    (is (= (set (all-statements tx)) #{["Also" a "test" _]}))
//    (cancel tx))))
//
//    (testing "removing rules from collections"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (let [tx (writeTx (collection store "test"))]
//    (add-rule tx ["This" a "test"])
//    (add-rule tx ["Also" a "test"])
//    (remove-rule tx ["This" a "test"])
//    (commit tx))
//    (let [tx (readTx (collection store "test"))]
//    (is (= (set (all-rules tx)) #{["Also" a "test"]}))
//    (cancel tx))))
//
//    (testing "matching statements in collections"
//    (let [store (ligature-memory-store)]
//    (is (not (= (create-collection store "test") nil))) ;TODO maybe check collection type instead of just making sure it's not null
//    (let [tx (writeTx (collection store "test"))]
//    (add-statement tx ["This" a "test" _])
//    (add-statement tx [(new-identifier tx) a "test" _])
//    (add-statement tx ["a" "knows" "b" _])
//    (add-statement tx ["b" "knows" "c" _])
//    (add-statement tx ["c" "knows" "a" _])
//    (add-statement tx ["c" "knows" "a" _]) ; dupe
//    (add-statement tx [(new-identifier tx) (new-identifier tx) (new-identifier tx) (new-identifier tx)])
//    (commit tx))
//    (let [tx (readTx (collection store "test"))]
//    (is (= (count (match-statements tx [:? :? :? :?])) 6))
//    (is (= (count (match-statements tx [:? :? :? _])) 5))
//    (is (= (set (match-statements tx [:? a :? :?])) #{["_:1" a "test" _] ["This" a "test" _]}))
//    (is (= (set (match-statements tx [:? a "test" :?])) #{["This" a "test" _] ["_:1" a "test" _]}))
//    (is (= (set (match-statements tx [:? :? "test" :?])) #{["This" a "test" _] ["_:1" a "test" _]}))
//    (is (= (set (match-statements tx [:? :? :? "_:5"])) #{["_:2" "_:3" "_:4" "_:5"]}))
//    (cancel tx))))) ; TODO add test running against a non-existant collection w/ match-statement calls
})
