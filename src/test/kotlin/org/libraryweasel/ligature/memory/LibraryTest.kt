/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import org.libraryweasel.ligature.*

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

    "new entity test" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addStatement(Statement(tx.newEntity(), tx.newEntity(), tx.newEntity(), tx.newEntity()))
        tx.addStatement(Statement(tx.newEntity(), tx.newEntity(), tx.newEntity(), tx.newEntity()))
        tx.commit()
        val readTx = collection.readTx()
        readTx.allStatements().toSet() shouldBe setOf(
                Statement(Entity("_:1"), Entity("_:2"), Entity("_:3"), Entity("_:4")),
                Statement(Entity("_:5"), Entity("_:6"), Entity("_:7"), Entity("_:8")))
        readTx.cancel()
    }

    "matching rules in collections" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addRule(Rule(Entity("This"), a, Entity("test")))
        tx.addRule(Rule(tx.newEntity(), a, Entity("test")))
        tx.addRule(Rule(Entity("a"), Entity("knows"), Entity("b")))
        tx.addRule(Rule(Entity("b"), Entity("knows"), Entity("c")))
        tx.addRule(Rule(Entity("c"), Entity("knows"), Entity("a")))
        tx.addRule(Rule(Entity("c"), Entity("knows"), Entity("a"))) //dupe
        tx.addRule(Rule(tx.newEntity(), tx.newEntity(), tx.newEntity()))
        tx.commit()
        val readTx = collection.readTx()
        readTx.matchRules().toSet().size shouldBe 6
        readTx.matchRules(null, null, null).toSet().size shouldBe 6
        readTx.matchRules(null, a, null).toSet() shouldBe setOf(
                Rule(Entity("This"), a, Entity("test")),
                Rule(Entity("_:1"), a, Entity("test"))
        )
        readTx.matchRules(null, a, "test").toSet() shouldBe setOf(
                Rule(Entity("This"), a, Entity("test")),
                Rule(Entity("_:1"), a, Entity("test"))
        )
        readTx.matchRules(null, null, "test").toSet() shouldBe setOf(
                Rule(Entity("This"), a, Entity("test")),
                Rule(Entity("_:1"), a, Entity("test"))
        )
        readTx.cancel() // TODO add test running against a non-existant collection w/ match-statement calls
    }

    "matching statements in collections" {
        val store = InMemoryStore()
        val collection = store.createCollection(Entity("test"))
        collection shouldNotBe null
        val tx = collection.writeTx()
        tx.addStatement(Statement(Entity("This"), a, Entity("test"), default))
        tx.addStatement(Statement(tx.newEntity(), a, Entity("test"), default))
        tx.addStatement(Statement(Entity("a"), Entity("knows"), Entity("b"), default))
        tx.addStatement(Statement(Entity("b"), Entity("knows"), Entity("c"), default))
        tx.addStatement(Statement(Entity("c"), Entity("knows"), Entity("a"), default))
        tx.addStatement(Statement(Entity("c"), Entity("knows"), Entity("a"), default)) //dupe
        tx.addStatement(Statement(tx.newEntity(), tx.newEntity(), tx.newEntity(), tx.newEntity()))
        tx.commit()
        val readTx = collection.readTx()
        readTx.matchStatements().toSet().size shouldBe 6
        readTx.matchStatements(null, null, null, default).toSet().size shouldBe 5
        readTx.matchStatements(null, a, null).toSet() shouldBe setOf(
                Statement(Entity("This"), a, Entity("test"), default),
                Statement(Entity("_:1"), a, Entity("test"), default)
        )
        readTx.matchStatements(null, a, "test").toSet() shouldBe setOf(
                Statement(Entity("This"), a, Entity("test"), default),
                Statement(Entity("_:1"), a, Entity("test"), default)
        )
        readTx.matchStatements(null, null, "test", null).toSet() shouldBe setOf(
                Statement(Entity("This"), a, Entity("test"), default),
                Statement(Entity("_:1"), a, Entity("test"), default)
        )
        readTx.matchStatements(null, null, null, Entity("_:5")).toSet() shouldBe setOf(
                Statement(Entity("_:2"), Entity("_:3"), Entity("_:4"), Entity("_:5"))
        )
        readTx.cancel() // TODO add test running against a non-existant collection w/ match-statement calls
        // TODO test range queries
    }
})
