/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.flow.toList
import org.libraryweasel.ligature.Entity
import org.libraryweasel.ligature.Statement
import org.libraryweasel.ligature.a
import org.libraryweasel.ligature.default

class InMemorySpec: StringSpec({
    "Create and close store" {
        val store = InMemoryStore()
        store.details() shouldBe mapOf("location" to "memory")
        store.allCollections().toList() shouldBe listOf<Entity>()
        store.close()
    }

    "access new collection" {
        val store = InMemoryStore()
        store.collection(Entity("test")) shouldNotBe  null
        store.allCollections().toList() shouldBe listOf<Entity>()
    }

    "creating a new collection" {
        val store = InMemoryStore()
        store.createCollection(Entity("test")) shouldNotBe  null
        store.allCollections().toList() shouldBe listOf<Entity>(Entity("test"))
    }

    "access and delete new collection" {
        val store = InMemoryStore()
        store.createCollection(Entity("test")) shouldNotBe  null
        store.allCollections().toList() shouldBe listOf<Entity>(Entity("test"))
        store.deleteCollection(Entity("test"))
        store.deleteCollection(Entity("test2"))
        store.allCollections().toList() shouldBe listOf<Entity>()
    }

    "new collections should be empty" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe  null
        val tx = collection.readTx()
        tx.allStatements().toList() shouldBe listOf()
        tx.allRules().toList() shouldBe listOf()
        tx.cancel()
    }

    "adding statements to collections" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addStatement(Statement(Entity("This"), a, Entity("test"), default))
        tx.commit()
        val readTx = collection.readTx()
        readTx.allStatements().toList() shouldBe listOf(Statement(Entity("This"), a, Entity("test"), default))
        readTx.cancel()
    }

    "adding rules to collections" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addRule(Rule(Entity("Also"), a, Entity("test")))
        tx.commit()
        val readTx = collection.readTx()
        readTx.allRules().toList() shouldBe listOf(Rule(Entity("Also"), a, Entity("test")))
        readTx.cancel()
    }

    "removing statements from collections" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addStatement(Statement(Entity("This"), a, Entity("test"), default))
        tx.addStatement(Statement(Entity("Also"), a, Entity("test"), default))
        tx.removeStatement(Statement(Entity("This"), a, Entity("test"), default))
        tx.commit()
        val readTx = collection.readTx()
        readTx.allStatements().toList() shouldBe listOf(Statement(Entity("Also"), a, Entity("test"), default))
        readTx.cancel()
    }

    "removing rules from collections" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addRule(Rule(Entity("This"), a, Entity("test")))
        tx.addRule(Rule(Entity("Also"), a, Entity("test")))
        tx.removeRule(Rule(Entity("This"), a, Entity("test")))
        tx.commit()
        val readTx = collection.readTx()
        readTx.allRules().toList() shouldBe listOf(Rule(Entity("Also"), a, Entity("test")))
        readTx.cancel()
   }

    "matching statements in collections" {
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
    }
})
