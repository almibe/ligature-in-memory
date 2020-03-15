/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import io.vavr.collection.HashSet
import io.vavr.collection.Set
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.libraryweasel.ligature.*
import java.lang.RuntimeException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

private data class CollectionValue(val statements: Set<Statement>, val  rules: Set<Rule>)

class InMemoryStore: LigatureStore {
    private val collections = ConcurrentHashMap<Entity, CollectionValue>()
    private val lock = ReentrantReadWriteLock()

    override fun allCollections(): Flow<Entity> = collections.keys.asFlow()

    override fun close() = collections.clear()

    override fun createCollection(collectionName: Entity): LigatureCollection {
        collections.putIfAbsent(collectionName, CollectionValue(HashSet.empty(), HashSet.empty()))
        return InMemoryCollection(collectionName, collections, lock)
    }

    override fun collection(collectionName: Entity): LigatureCollection =
        InMemoryCollection(collectionName, collections, lock)

    override fun deleteCollection(collectionName: Entity) {
        collections.remove(collectionName)
    }

    override fun details(): Map<String, String> = mapOf("location" to "memory")
}

private class InMemoryCollection(private val name: Entity,
                                 private val collections: ConcurrentHashMap<Entity, CollectionValue>,
                                 private val lock: ReentrantReadWriteLock): LigatureCollection {
    override val collectionName: Entity
        get() = name

    override fun readTx(): ReadTx = InMemoryReadTx(name, collections, lock)

    override fun writeTx(): WriteTx = InMemoryWriteTx(name, collections, lock)
}

private class InMemoryReadTx(name: Entity,
                             collections: ConcurrentHashMap<Entity, CollectionValue>,
                             lock: ReentrantReadWriteLock): ReadTx {
    private val collection = collections[name]
    private val active = AtomicBoolean(true)
    private val readLock = lock.readLock()

    init {
        readLock.lock()
    }

    override fun allRules(): Flow<Rule> {
        return if (active.get()) {
            collection?.rules?.asFlow() ?: listOf<Rule>().asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun allStatements(): Flow<Statement> {
        return if (active.get()) {
            collection?.statements?.asFlow() ?: listOf<Statement>().asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun cancel() {
        if (active.get()) {
            readLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchRules(subject: Entity?, predicate: Entity?, `object`: Node?): Flow<Rule> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, `object`: Node?, graph: Entity?): Flow<Statement> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, range: Range<*>, graph: Entity?): Flow<Statement> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}

private class InMemoryWriteTx(name: Entity,
                              collections: ConcurrentHashMap<Entity, CollectionValue>,
                              lock: ReentrantReadWriteLock): WriteTx {
    private val active = AtomicBoolean(true)
    private val writeLock = lock.writeLock()

    init {
        writeLock.lock()
    }

    override fun addRule(rule: Rule) {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun addStatement(statement: Statement) {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun allRules(): Flow<Rule> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun allStatements(): Flow<Statement> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun cancel() {
        if (active.get()) {
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun commit() {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchRules(subject: Entity?, predicate: Entity?, `object`: Node?): Flow<Rule> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, `object`: Node?, graph: Entity?): Flow<Statement> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, range: Range<*>, graph: Entity?): Flow<Statement> {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun newEntity(): Entity {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun removeRule(rule: Rule) {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun removeStatement(statement: Statement) {
        if (active.get()) {
            TODO("Not yet implemented")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}
