/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.memory

import io.vavr.collection.HashSet
import io.vavr.collection.Set
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import dev.ligature.*
import java.lang.RuntimeException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

private data class CollectionValue(val statements: Set<PersistedStatement>,
                                   val counter: AtomicLong)

class InMemoryStore: LigatureStore {
    private val collections = ConcurrentHashMap<CollectionName, CollectionValue>()
    private val lock = ReentrantReadWriteLock()
    private val open = AtomicBoolean(true)

    override suspend fun close() {
        open.set(false)
        collections.clear()
    }

    override suspend fun readTx(): ReadTx {
        if (open.get()) {
            return InMemoryReadTx(collections, lock)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override suspend fun writeTx(): WriteTx {
        if (open.get()) {
            return InMemoryWriteTx(collections, lock)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override suspend fun isOpen(): Boolean = open.get()
}

private class InMemoryReadTx(private val collections: ConcurrentHashMap<CollectionName, CollectionValue>,
                             private val lock: ReentrantReadWriteLock): ReadTx {
    private val readLock = lock.readLock()
    private val active = AtomicBoolean(true)

    init {
        readLock.lock()
    }

    override suspend fun allStatements(collection: CollectionName): Flow<PersistedStatement> {
        if (active.get()) {
            val result = collections[collection]?.statements?.toSet()?.asFlow()
            return result ?: setOf<PersistedStatement>().asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override suspend fun cancel() {
        if (active.get()) {
            readLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override suspend fun collections(): Flow<CollectionName> = collections.keys.asFlow()

    override suspend fun collections(prefix: CollectionName): Flow<CollectionName> = collectionsImpl(collections, prefix)

    override suspend fun collections(from: CollectionName, to: CollectionName): Flow<CollectionName> = collectionsImpl(collections, from, to)

    override suspend fun isOpen(): Boolean = active.get()

    override suspend fun matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, `object`: Object?): Flow<PersistedStatement> {
        return if (active.get()) {
            if (collections.containsKey(collection)) {
                matchStatementsImpl(collections[collection]!!.statements, subject, predicate, `object`)
            } else {
                setOf<Statement>().asFlow()
            }
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override suspend fun matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, range: Range<*>): Flow<PersistedStatement> {
        return if (active.get()) {
            if (collections.containsKey(collection)) {
                matchStatementsImpl(collections[collection]!!.statements, subject, predicate, range)
            } else {
                setOf<Statement>().asFlow()
            }
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}

private class InMemoryWriteTx(private val collections: ConcurrentHashMap<CollectionName, CollectionValue>,
                              private val lock: ReentrantReadWriteLock): WriteTx {
    private val writeLock = lock.writeLock()
    private val active = AtomicBoolean(true)
    private val workingState = ConcurrentHashMap(collections)

    init {
        writeLock.lock()
    }

    @Synchronized override suspend fun addStatement(collection: CollectionName, statement: Statement): PersistedStatement {
        if (active.get()) {
            createCollection(collection)
            val context = newEntity(collection)
            val persistedStatement = PersistedStatement(collection, statement, context)
            workingState[collection] = CollectionValue(workingState[collection]!!.statements.add(persistedStatement), workingState[collection]!!.counter)
            return persistedStatement
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override suspend fun cancel() {
        if (active.get()) {
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override suspend fun commit() {
        if (active.get()) {
            collections.clear()
            collections.putAll(workingState)
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override suspend fun createCollection(collection: CollectionName) {
        if (active.get()) {
            workingState.putIfAbsent(collection, CollectionValue(HashSet.empty(), AtomicLong(0)))
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override suspend fun deleteCollection(collection: CollectionName) {
        if (active.get()) {
            workingState.remove(collection)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override suspend fun isOpen(): Boolean = active.get()

    @Synchronized override suspend fun newEntity(collection: CollectionName): AnonymousEntity {
        if (active.get()) {
            createCollection(collection)
            val newId = workingState[collection]!!.counter.incrementAndGet()
            workingState[collection] = CollectionValue(workingState[collection]!!.statements, workingState[collection]!!.counter)
            return AnonymousEntity(newId)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override suspend fun removeEntity(collection: CollectionName, entity: Entity) {
        TODO("Not yet implemented")
    }

    override suspend fun removePredicate(collection: CollectionName, predicate: Predicate) {
        TODO("Not yet implemented")
    }

    @Synchronized override suspend fun removeStatement(collection: CollectionName, statement: Statement) {
        if (active.get()) {
            if (workingState.containsKey(collection)) {
                workingState[collection] = CollectionValue(workingState[collection]!!.statements.remove(statement), workingState[collection]!!.counter)
            }
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}

private fun collectionsImpl(collections: ConcurrentHashMap<CollectionName, CollectionValue>, prefix: CollectionName): Flow<CollectionName> {
    return collections.keys.asFlow().filter {
        it != null && it.identifier.startsWith(prefix.identifier)
    }
}

private fun collectionsImpl(collections: ConcurrentHashMap<CollectionName, CollectionValue>, from: CollectionName, to: CollectionName): Flow<CollectionName> {
    return collections.keys.asFlow().filter {
        it != null && it.identifier >= from.identifier && it.identifier < to.identifier
    }
}

private fun matchStatementsImpl(statements: Set<Statement>, subject: Entity?, predicate: Predicate?, `object`: Object?): Flow<Statement> {
    return statements.asFlow().filter {
        when (subject) {
            null -> true
            else -> (subject == it.subject)
        }
    }.filter {
        when (predicate) {
            null -> true
            else -> (predicate == it.predicate)
        }
    }.filter {
        when (`object`) {
            null -> true
            else -> (`object` == it.`object`)
        }
    }
}

private fun matchStatementsImpl(statements: Set<Statement>, subject: Entity?, predicate: Predicate?, range: Range<*>): Flow<Statement> {
    return statements.asFlow().filter {
        when (subject) {
            null -> true
            else -> (subject == it.subject)
        }
    }.filter {
        when (predicate) {
            null -> true
            else -> (predicate == it.predicate)
        }
    }.filter {
        when (range) {
            is LangLiteralRange -> (it.`object` is LangLiteral && ((it.`object` as LangLiteral).langTag == range.start.langTag && range.start.langTag == range.end.langTag) && (it.`object` as LangLiteral).value >= range.start.value && (it.`object` as LangLiteral).value < range.end.value)
            is StringLiteralRange -> (it.`object` is StringLiteral && (it.`object` as StringLiteral).value >= range.start && (it.`object` as StringLiteral).value < range.end)
            is LongLiteralRange -> (it.`object` is LongLiteral && (it.`object` as LongLiteral).value >= range.start && (it.`object` as LongLiteral).value < range.end)
            is DoubleLiteralRange -> (it.`object` is DoubleLiteral && (it.`object` as DoubleLiteral).value >= range.start && (it.`object` as DoubleLiteral).value < range.end)
        }
    }
}
